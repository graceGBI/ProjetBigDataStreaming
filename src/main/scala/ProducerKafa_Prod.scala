import KafkaStreaming.{getJSON, getKafkaConsumerParams, getKafkaProducerParams_exactly_once, trace_kafka}
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.security.auth.SecurityProtocol

import java.util.{Collections, Properties}
import org.apache.kafka.clients.producer._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext

import java.time.Duration
import java.util
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object ProducerKafa_Prod {

  private var trace_kafka : Logger = LogManager.getLogger("Log_Console")

  def main(args: Array[String]): Unit = {

    val kafkaBootstrapServers : String = "localhost:9092"
    val topic_name : String = "orderline"

    for(i<-1 to 10 ){
      ProducerKafka_prod(kafkaBootstrapServers,topic_name)
    }
  }

  def ProducerKafka_prod(kafkaBootstrapServers : String,topic_name : String) : Unit={

    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs :  ${kafkaBootstrapServers}")
    val prodcucer_kafka = new KafkaProducer[String,String](getKafkaProducerParams_prod(kafkaBootstrapServers))

    val record_publish = getJSON(topic_name)

    try{
      trace_kafka.info("publication du message")
      prodcucer_kafka.send(record_publish,new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            //le message a été enregistré dans Kafka sans problème. Je peux récupérer les infos envoyées
            println("offset du message : " + metadata.offset().toString)
            println("topic du message : " + metadata.topic().toString())
            println("partition du message : " + metadata.partition().toString())
            println("heure d'enregistrement du message : " + metadata.timestamp())
          }
        }
      })
      println("message publié avec succès ! :)")
    }catch {
      case ex: Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
        trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams_prod(kafkaBootstrapServers))
    }
  }

  def getKafkaProducerParams_prod (KafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootStrapServers)
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384")
    props.put(ProducerConfig.LINGER_MS_CONFIG,"100")
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"gzip")
    //dans le jass file props.put("security.protocol",  "SASL_PLAINTEXT")
    return props

  }

  def getJSON(topic_name : String) : ProducerRecord[String, String] ={
    val objet_json = JsonNodeFactory.instance.objectNode()

    val price : Int = 45

    objet_json.put("orderid", "10")
    objet_json.put("customerid", "CLI15")
    objet_json.put("campaignid", "CAMP30")
    objet_json.put("orderdate", "03/05/2021")
    objet_json.put("city", "Paris")
    objet_json.put("state", "RAS")
    objet_json.put("zipcode", "75005")
    objet_json.put("paymenttype", "CB")
    objet_json.put("totalprice", price)
    objet_json.put("numorderlines", 200)
    objet_json.put("numunit",10)

    println("L'objet JSON est : %s".format(objet_json.toString))

    return  new ProducerRecord[String, String](topic_name,objet_json.toString)
  }

}
