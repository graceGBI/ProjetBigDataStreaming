import ConsommationStreaming.schema_Kafka
import org.apache.log4j.{LogManager, Logger}
import KafkaStreaming._
import SparkBigData._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
  val scc = getSparkStreamingContext(true, batch_duration)

  private var trace_consommation : Logger = LogManager.getLogger("Log_Console")

  val schema_Kafka = StructType(Array(
    StructField("Zipcode", IntegerType, true),
    StructField("ZipCodeType", StringType, true),
    StructField("City", StringType, true),
    StructField("State", StringType, true)
  ))

  def main(args: Array[String]): Unit = {

    val kafkaStreams = getConsommateurKafka(bootstrapServers,consumerGroupId,ordre_lecture,consumerReadOrder,zookeeper,kerberosName,topics, scc)

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
}
