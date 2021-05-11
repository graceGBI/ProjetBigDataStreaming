import ConsommationStreaming.{batch_duration, bootstrapServers, consumerGroupId, consumerReadOrder, kerberosName, topics, trace_consommation, zookeeper}
import KafkaStreaming.getConsommateurKafka
import SparkBigData.getSparkStreamingContext
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, from_json, from_unixtime, lit, unix_timestamp}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object IndicateursStreaming {

  val  schema_indicateurs: StructType = StructType(Array(
    StructField("event_date", DateType, true),
    StructField("id", StringType, false),
    StructField("text", StringType, true),
    StructField("lang", StringType, true),
    StructField("userid", StringType, false),
    StructField("name", StringType, false),
    StructField("screenName", StringType, true),
    StructField("location", StringType, true),
    StructField("followersCount", IntegerType, false),
    StructField("retweetCount", IntegerType, false),
    StructField("favoriteCount", IntegerType, false),
    StructField("Zipcode", StringType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true))
  )
  private val bootstrapServers : String = ""
  private val consumerGroupId : String = ""
  private val ordre_lecture : String = ""
  private val consumerReadOrder : String = ""
  private val zookeeper : String = ""
  private val kerberosName : String = ""
  private val batch_duration : Int =600 //10min
  private val topics : Array[String] = Array("")
  private val path_fichiers_kpi : String = "/data lake/marketing_JVC/Projet Streaming/kpi" //HDFS
  private var logger : Logger = LogManager.getLogger("Log_Console")

  def main(args: Array[String]): Unit = {

    val ssc = getSparkStreamingContext(true, batch_duration)

    val kk_consumer = getConsommateurKafka(bootstrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc)

    kk_consumer.foreachRDD {
      rddKafka => {
        try {
          if (!rddKafka.isEmpty()) {

            logger.info("récupération des datas,...")
            val event_kafka = rddKafka.map(record => record.value())
            //là l'offset est en mémoire, il faut donc choisir la queue ou une bdd externe
            val offset_kafka = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges

            //Instance de la session Spark: on passe le contexte spark utilisé par le rdd
            val ssession = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport.getOrCreate()
            import ssession.implicits._
            //valeur par défaut _col1
            val events_df = event_kafka.toDF("kafka_jsons") // Convertir les RDD en DataFrame
            // ici c'est la reception des data. On envoie aussi un jeton de reconnaissance à kafka
            if (events_df.count() == 0) {

              Seq(
                "Aucun évenement n'a été receptionné dans le quart d'heure"
              ).toDF("libellé")
                .coalesce(1) //coalesce ou repartition : regrouper tous les fichiers en 1 seule car chaque noeud du cluster spark traite chaque partie de la requête
                //donc lorsque chak noeud aura fini de traiter sa partie ils renvoient chaque bout à la fin du traitement au driver(machine principal) qui va fusionner en 1 seul fichier
                //sur des fichier volumineux vaux mieux faire le repartiion
                .write //write pour l'écriture,
                .format("com.databricks.spark.csv")
                .mode(SaveMode.Overwrite) //Mode Overwrite pour remplacer le fichier existant
                .save(path_fichiers_kpi + "/indicateur.csv")

            } else {

              val df_parsed = getParsedData(events_df, ssession)
              val df_kpis = getIndicateursComputed(df_parsed, ssession).cache()
              //Etant donner, qu'on a beaucoup d'agrégation, d'itération entre les data dans le calcul de notre requête sql,
              // on doit essayer d'augmenter la performance pour éviter des latences. Donc de demander à Spark de mettre les data intermédaire en cache

              df_kpis.repartition(1) //un tasktraker
                .write
                .format("com.databricks.spark.csv")
                .mode(SaveMode.Append)
                .save(path_fichiers_kpi + "/indicateurs_streaming.csv")
            }

            // sémantique de livraison et de traitement exactement une fois. Persistance des offsets dans Kafka à la fin du traitement
            logger.info("persistance des offsets dans Kafka encours....")
            kk_consumer.asInstanceOf[CanCommitOffsets].commitAsync(offset_kafka)
            logger.info("persistance des offsets dans Kafka terminé avec succès ! :) ")
          }

        }catch {

          case ex : Exception => logger.error(s"il y'a une ereur dans l'application ${ex.printStackTrace()}")

        }
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

  def getParsedData (kafkaEventsDf : DataFrame, ss : SparkSession) : DataFrame = {

    logger.info("parsing des json reçus de Kafka encours...")

    import ss.implicits._

    val df_events = kafkaEventsDf.withColumn("kafka_jsons",from_json(col("kakfa_jsons"),schema_indicateurs))
      .filter(col("kafka_jsons.lang") === lit("en") || col("kafka_jsons.lang") === lit("fr"))
      .select(
        col("kafka_jsons.event_date"),
        col("kafka_jsons.id"),
        col("kafka_jsons.text"),
        col("kafka_jsons.lang"),
        col("kafka_jsons.userid"),
        col("kafka_jsons.name"),
        col("kafka_jsons.screenName"),
        col("kafka_jsons.location"),
        col("kafka_jsons.followersCount"),
        col("kafka_jsons.retweetCount"),
        col("kafka_jsons.favoriteCount"),
        col("kafka_jsons.Zipcode"),
        col("kafka_jsons.ZipCodeType"),
        col("kafka_jsons.City"),
        col("kafka_jsons.State")
      )

    return df_events
  }

  def getIndicateursComputed(eventsDf_parsed: DataFrame, ss : SparkSession) : DataFrame = {
    logger.info("calcul des indicateurs...")

    //la date de l'évenement
    from_unixtime(unix_timestamp(col("event_date")),"dd-mm-yyyy")

    //le quart d'heure : 4 cas sont possibles : HH:00-HH:15, HH:15-HH:30, HH:30-HH:45, HH:45-HH:59
    //concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "00"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "15")

    import ss.implicits._

    eventsDf_parsed.createOrReplaceTempView("events_tweets") // créer une vue temporaire sql

    val df_indicateurs = ss.sql("""
           SELECT t.date_event,
               t.quart_heure,
               count(t.id) OVER (PARTITION BY t.date_event, t.quart_heure ORDER BY t.date_event,t.quart_heure) as tweetCount,
               sum(t.bin_retweet) OVER (PARTITION BY t.date_event, t.quart_heure ORDER BY t.date_event, t.quart_heure) as retweetCount

           FROM  (
              SELECT  from_unixtime(unix_timestamp(event_date, 'yyyy-MM-dd'), 'yyyy-MM-dd') as date_event,
                  CASE
                    WHEN minute(event_date) < 15 THEN CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "00"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "15"))
                    WHEN minute(event_date) between 15 AND 29 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "15"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "30"))
                    WHEN minute(event_date) between 30 AND 44 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "30"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "45"))
                    WHEN minute(event_date) > 44 THEN  CONCAT(concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "45"), " - ", concat(from_unixtime(unix_timestamp(cast(hour(event_date) as string), 'HH'), 'HH'), ":", "60"))
                  END AS quart_heure,
                  CASE
                    WHEN retweetCount > 0 THEN 1
                    ELSE 0
                  END AS bin_retweet,
                   id
              FROM events_tweets
           ) t ORDER BY t.quart_heure
       """).withColumn("Niveau_RT", round(lit(col("retweetCount") / col("tweetCount")) * lit(100), 2))
          .withColumn("date_event", when(col("date_event").isNull, current_timestamp()).otherwise(col("date_event")))
          .select(
            col("date_event").alias("Date de l'event"),
            col("quart_heure").alias("Quart d'heure de l'event"),
            col("tweetCount").alias("Nbre de Tweets par QH"),
            col("retweetCount").alias("Nbre de Retweets par QH"),
            col("Niveau_RT").alias("Niveau de ReTweet (en %)")
          )

    return df_indicateurs
  }

}
