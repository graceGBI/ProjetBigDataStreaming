
import org.apache.log4j.{LogManager, Logger}
import KafkaStreaming._
import SparkBigData._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object ConsommationStreaming {

  val bootstrapServers : String = ""
  val consumerGroupId : String = ""
  val ordre_lecture : String = ""
  val consumerReadOrder : String = ""
  val zookeeper : String = ""
  val kerberosName : String = ""
  val batch_duration : Int =15
  val topics : Array[String] = Array("")
  val chekpointChemin : String = "/Hadoop/mhgb/datalake/"
  val scc = getSparkStreamingContext(true, batch_duration)

  private var trace_consommation : Logger = LogManager.getLogger("Log_Console")

  val schema_Kafka = StructType(Array(
    StructField("Zipcode", IntegerType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)
  ))

  /**
   * checkpointing avec Spark Streaming
   * @param checkpointPath : chemin d'enregistrement du checkpoint
   * @return : context spark streaming avec prise en compte du checkpoint
   */
  def fault_tolerant_SparkStreamingContext (checkpointPath : String) : StreamingContext = {

    val ssc2 = getSparkStreamingContext(true, batch_duration)

    val kafkaStreams_cp = getConsommateurKafka(bootstrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc2)

    ssc2.checkpoint(checkpointPath)

    return ssc2

  }
  def main(args: Array[String]): Unit = {
    persister_in_queue_kafka ()

  }

  def persister_in_queue_kafka () : Unit ={

    val kafkaStreams = getConsommateurKafka(bootstrapServers,consumerGroupId,consumerReadOrder,zookeeper,kerberosName,topics, scc)

    // on a toutes les méthodes des RDD, le kafkastream contient les consumerecord
    //1ère méthode : lecture simple des données, elle n'implique aucune gestion de l'offset
    //Donc on n'a pas le contrôle sur le niveau de sémantique de l'application
    //Dans la pratique, elle est rarement utiliser
    //val dataStreams = kafkaStreams.map(record=> record.value())

    // deuxième méthode (recommandée)
    kafkaStreams.foreachRDD {
      rddKafka => {
        if (!rddKafka.isEmpty()) {
          //gestion des offset
          val offsets = rddKafka.asInstanceOf[HasOffsetRanges].offsetRanges //là l'offset est en mémoire, il faut donc choisir la queue ou une bdd externe

          //lire les offset
          for(o <- offsets){
            println()
            trace_consommation.info(s"Le topic lu est :  o.topic la partition est : o.partition l'offset de début est : o.fromOffset l'offset de fin est : o.untilOffset")
          }

          val dataStreams = rddKafka.map(record => record.value())

          //Instance de la session Spark: on passe le contexte spark utilisé par le rdd
          val ss = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport.getOrCreate()
          import ss.implicits._
          //valeur par défaut _col1
          val df_kafka = dataStreams.toDF("tweet_message")  // Convertir les RDD en DataFrame
          // ici c'est la reception des data. On envoie aussi un jeton de reconnaissance à kafka


          df_kafka.createOrReplaceTempView("kafka_events")     // créer une vue temporaire sql

          // 1 ère méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka = ss.sql("select * from kafka_events")

          df_eventsKafka.show()

          // On sait qu'on a les data dans un fichier json mais on ne connait pas la structure des données
          // 2 ème méthode d'exploitation du Data Frame et SQL avec Kafka
          val df_eventsKafka_2 = df_kafka.withColumn("tweet_message", from_json(col("tweet_message"), schema_Kafka))
            .select(col("tweet_message.*"))
          /*OU
            val df_eventsKafka_2 = df_kafka.withColumn("tweet_message", from_json(col("tweet_message"), schema_Kafka))
              .select(col("tweet_message.Zipcode"))
              .select(col("tweet_message.ZipcodeType"))
              .select(col("tweet_message.State"))**/

          // sémantique de livraison et de traitement exactement une fois. Persistance des offsets dans Kafka à la fin du traitement
          trace_consommation.info("persistance des offsets dans Kafka encours....")
          kafkaStreams.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
          trace_consommation.info("persistance des offsets dans Kafka terminé avec succès ! :) ")


        }
      }
    }

    scc.start()
    scc.awaitTermination()
  }

  def execution_checkPoint () : Unit = {

    val ssc_cp = StreamingContext.getOrCreate(chekpointChemin, () => fault_tolerant_SparkStreamingContext(chekpointChemin))

    val kafkaStreams_cp = getConsommateurKafka(bootstrapServers, consumerGroupId, consumerReadOrder, zookeeper, kerberosName, topics, ssc_cp)

    kafkaStreams_cp.checkpoint(Seconds(15))

    kafkaStreams_cp.foreachRDD {

      rddKafka => {
        if (!rddKafka.isEmpty()) {

          val dataStreams = rddKafka.map(record => record.value())

          val ss = SparkSession.builder.config(rddKafka.sparkContext.getConf).enableHiveSupport.getOrCreate()
          import ss.implicits._

          val df_kafka = dataStreams.toDF("tweet_message")

          val df_eventsKafka_2 = df_kafka.withColumn("tweet_message", from_json(col("tweet_message"), schema_Kafka))
            .select(col("tweet_message.*"))

        }

      }

    }

    ssc_cp.start()
    ssc_cp.awaitTermination()

  }

  /**
   * La fonction streaming. Si on déploie notre application c'est OK. Le reste ce sont des traitements
   * Le micro batch est déclencher par le SparkStreamingContext avec son batchduration
   */
  def streamingCas () : Unit = {

    val ssc = getSparkStreamingContext(true, batch_duration)

    //on ajoute les traitements que l'on souhaite, kafka et autres
    //Kafka fait du pull, il est à l'écoute de la source streaming dès qu'un evt arrive il le recupère
    //Par contre Spark non, il ne vas pas récupérer les data, on doit les lui envoyer.
    // A grande échelle (des centaines de milliers et plus,..) si le débit des évt est trop importantes
    // ou que la source streaming RGBD Salesforce dans notre cas n'arrive pas à supporter le niveau de parallélisme,
    // la charge de calcul, le nbre de user connecté dans Spark.
    //Donc c'est mieux d'avoir un système d'ingestion streaming.

    ssc.start()
    ssc.awaitTermination()

  }

}
