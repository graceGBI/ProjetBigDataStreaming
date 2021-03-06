import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils._
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.security.auth.SecurityProtocol

import java.util.{Collections, Properties}
import org.apache.kafka.clients.producer._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.StreamingContext

import java.time.Duration
import java.util
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object KafkaStreaming {

  var kafkaParam: Map[String, Object] = Map(null, null)
  private var trace_kafka : Logger = LogManager.getLogger("Log_Console")

  /**
   * cette fonction récupère les paramètres de connexion à un cluster Kafka
   * @param kafkaBootstrapServers: adresses IP (avec port) des agents du cluster Kafka
   * @param kafkaConsumerGroupId: c'est l'ID du consumer group
   * @param ordre_lecture : l'ordre de lecture du Log
   * @param kafkaConsumerReadOrder: l'adresse IP (avec port) de l'ensemble ZooKeeper
   * @param kafkaZookeeper: le nom du service Kerberos
   * @param kerberosName : le nom du service Kerberos
   * @return: la fonction renvoie une table clé-valeur des paramètres de connexion à un cluster Kafka spécifique
   */
  def getKafkaSparkConsumerParams(kafkaBootstrapServers : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                                  kafkaZookeeper : String , kerberosName : String): Map[String, Object] ={

    kafkaParam = Map(
      "bootstrap.servers" -> kafkaBootstrapServers,
      "group.id"  -> kafkaConsumerGroupId,
      "zookeeper.hosts" -> kafkaZookeeper,
      "auto.offset.reset" -> kafkaConsumerReadOrder,
      // à false c'est à nous de le gérer et à true non
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "sasl.kerberos.service.name" -> kerberosName,
      "security.protocol" -> SecurityProtocol.PLAINTEXT
    )
    return kafkaParam
  }

  /**
   *Consommateur kafka à l'aide de spark Streaming
   * @param kafkaBootstrapServers :  adresse IP des agents Kafka
   * @param kafkaConsumerGroupId : ID du consummer Group
   * @param ordre_lecture :
   * @param kafkaConsumerReadOrder : ordre de lecture des données du Log
   * @param kafkaZookeeper : ensemble Zookeeper
   * @param kerberosName : service kerberos
   * @param kafkaTopics : le nom des topics
   * @param batch_duration
   * @return
   */
  def getConsommateurKafka(kafkaBootstrapServers : String, kafkaConsumerGroupId : String, kafkaConsumerReadOrder : String,
                           kafkaZookeeper : String , kerberosName : String,
                           kafkaTopics : Array[String], streamContext : StreamingContext) : InputDStream[ConsumerRecord[String,String]] = {

    val ssc = streamContext
    kafkaParam=getKafkaSparkConsumerParams(kafkaBootstrapServers,kafkaConsumerGroupId,kafkaConsumerReadOrder,kafkaZookeeper,kerberosName)

    //c'est un ConsumerRecord mais de type InputDStream
    //Souscrire à kakfa, récupère les données du topic1 dans le cluster kafka via les infos paramétrées
    //Ensuite il renvoie 3 infos : les data du topic, la partition où il recupère les data et l'offset
    var consummateurKafka: InputDStream[ConsumerRecord[String, String]] = null
    try{
      //Les données reçues par kafka arrivent directement dans le RDD
      // Donc sans passer par un receiver donc on est sur une sémantique de livraison Au-Moins-Une-Fois
      // Donc au cas d'erreur, si on relance l'appli on peut relire les mêmes données plusieurs fois
      consummateurKafka =  KafkaUtils.createDirectStream[String,String](
        ssc,
        PreferConsistent,
        Subscribe[String,String](kafkaTopics,kafkaParam)
      )
    }catch {
      case ex:Exception =>
        trace_kafka.error(s"erreur dans l'initialisation du consumer Kafka ${ex.printStackTrace()}")
        trace_kafka.info(s"La liste des paramètres pour la connexion du consommateur Kafka sont : ${kafkaParam}")
    }

    return consummateurKafka
  }
  def getKafkaConsumerParams( kafkaBootStrapServers : String, kafkaConsumerGroupId : String) : Properties= {
    val props : Properties = new Properties()
    props.put("bootstrap.servers", kafkaBootStrapServers)
    props.put("auto.offset.reset", "latest")
    props.put("group.id",kafkaConsumerGroupId )
    props.put("enable.auto.commit", "false")
    props.put("key.serializer", "org.apache.kafka.common.serialization.Deserializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.Deserializer")

    return props

  }

  /**
   *
   * @param kafkaBootStrapServers :  adresse IP des agents Kafka
   * @param kafkaConsumerGroupId : ID du consummer Group
   * @param topic_list : la liste des topics
   * @return
   */
  def getClientConsumerKafka(kafkaBootStrapServers : String, kafkaConsumerGroupId : String, topic_list :String) : KafkaConsumer[String,String] ={
    trace_kafka.info("instanciation d'un consommateur Kafka...")
    val consumer  = new KafkaConsumer[String,String](getKafkaConsumerParams(kafkaBootStrapServers, kafkaConsumerGroupId))
    try {
      consumer.subscribe(Collections.singletonList(topic_list))
      while (true) {
        val messages: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(30)) //temps écroulé avant de récupérer les messages
        if (!messages.isEmpty) {
          trace_kafka.info("Nombre de messages collectés dans la fenêtre :" + messages.count())
          for (message <- messages.asScala) {
            println("Topic: " + message.topic() +
              ",Key: " + message.key() +
              ",Value: " + message.value() +
              ", Offset: " + message.offset() +
              ", Partition: " + message.partition())
          }

          try {
            consumer.commitAsync() // ou bien consumer.commitSync()
          } catch {
            case ex: CommitFailedException =>
              trace_kafka.error("erreur dans le commit des offset. Kafka n'a pas reçu le jeton de reconnaissance confirmant que nous avons bien reçu les données")
          }

        }
        //Méthode de lecture 2
        /** val messageIterator = messages.iterator()
         *  while (messageIterator.hasNext == true){
         *  val msg = messageIterator.next()
         *  println("Topic: " + msg.topic() +
         *  ",Key: " + msg.key() +
         *  ",Value: " + msg.value() +
         *  ", Offset: " + msg.offset() +
         *  ", Partition: " + msg.partition())
         *  }* */
      }
    }catch {
      case excpt : Exception =>
      trace_kafka.error("erreur dans le consumer" + excpt.printStackTrace())
    } finally {
    consumer.close()
    }
    return consumer
  }

  def getKafkaProducerParams( kafkaBootStrapServers : String) : Properties= {
    val props : Properties = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("bootstrap.servers ", kafkaBootStrapServers)
    props.put("security.protocol",  "SASL_PLAINTEXT")

    return props
  }


  def getKafkaProducerParams_exactly_once (KafkaBootStrapServers : String) : Properties = {
    val props : Properties = new Properties()
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBootStrapServers)
    props.put("security.protocol",  "SASL_PLAINTEXT")
    //propriétés pour rendre le producer Exactly-Once
    props.put(ProducerConfig.ACKS_CONFIG,  "all")
    // pour la cohérence éventuelle. Doit être inférieur ou égal au facteur de réplication du topic dans lequel vous allez publier
    props.put("min.insync.replicas", "2")
    //rendre le producer idempotent
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, "3")
    props.put("max.in.flight.requests.per.connection", "3")

    return props

  }

  /**
   * création d'un Kafka Producer qui va être déployé en production
   * @param kafkaBootstrapServers : agents kafka sur lesquels publier le message
   * @param topic_name : topic dans lequel publier le message
   * @param message :  message à publier dans le topic @topic_name
   * @return : renvoie un Producer Kafka
   */
  def getProducerKafka(kafkaBootstrapServers : String,topic_name : String, message : String) : KafkaProducer[String,String] ={

    trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs :  ${kafkaBootstrapServers}")
    val prodcucer_kafka = new KafkaProducer[String,String](getKafkaProducerParams(kafkaBootstrapServers))

    trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")
    val cle : String = "1" //pour que kafka regroupe tous les messages de cet id dans le même topic
                          // on peut déclarer la clé au niveau des paramètres, afin de créer des clé différentes
                          // par messsage au niveau de l'application pour notre cas d'usage c'est Twitter
    val record_publish = new ProducerRecord[String, String](topic_name,cle, message)

    try{
      trace_kafka.info("publication du message")
      prodcucer_kafka.send(record_publish)
      trace_kafka.info("message publié avec succès ! :)")
    }catch {
      case ex: Exception =>
        trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
      trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams(kafkaBootstrapServers))
    } finally {
      println("n'oubliez pas de clôturer le Producer à la fin de son utilisation")
     // prodcucer_kafka.close() sera fait par le client qui l'appelle
    }
    return prodcucer_kafka

  }

  def getJSON(topic_name : String) : ProducerRecord[String, String] ={
    val objet_json = JsonNodeFactory.instance.objectNode()

    val price : Int = 45

    objet_json.put("orderid", "")
    objet_json.put("customerid", "")
    objet_json.put("campaignid", "")
    objet_json.put("orderdate", "")
    objet_json.put("city", "")
    objet_json.put("state", "")
    objet_json.put("zipcode", "")
    objet_json.put("paymenttype", "CB")
    objet_json.put("totalprice", price)
    objet_json.put("numorderlines", 200)
    objet_json.put("numunit",10)

    println("L'objet JSON est : %s".format(objet_json.toString))

    return  new ProducerRecord[String, String](topic_name,objet_json.toString)
  }

 def getProducerKafka_exactly_once(kafkaBootstrapServers : String,topic_name : String, message : String) : KafkaProducer[String,String]={
   trace_kafka.info(s"instanciation d'une instance du producer Kafka aux serveurs :  ${kafkaBootstrapServers}")
   val prodcucer_kafka = new KafkaProducer[String,String](getKafkaProducerParams_exactly_once(kafkaBootstrapServers))

   trace_kafka.info(s"message à publier dans le topic ${topic_name}, ${message}")

   val record_publish = getJSON(topic_name)

   try{
     trace_kafka.info("publication du message")
     prodcucer_kafka.send(record_publish,new Callback {
       override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
         if (exception == null) {
           //le message a été enregistré dans Kafka sans problème. Je peux récupérer les infos envoyées
           trace_kafka.info("offset du message : " + metadata.offset().toString)
           trace_kafka.info("topic du message : " + metadata.topic().toString())
           trace_kafka.info("partition du message : " + metadata.partition().toString())
           trace_kafka.info("heure d'enregistrement du message : " + metadata.timestamp())
         }
       }
     })
     trace_kafka.info("message publié avec succès ! :)")
   }catch {
     case ex: Exception =>
       trace_kafka.error(s"erreur dans la publication du message dans Kafka ${ex.printStackTrace()}")
       trace_kafka.info("La liste des paramètres pour la connexion du Producer Kafka sont :" + getKafkaProducerParams_exactly_once(kafkaBootstrapServers))
   } finally {
     println("n'oubliez pas de clôturer le Producer à la fin de son utilisation")
     // prodcucer_kafka.close() sera fait par le client qui l'appelle
   }
   return prodcucer_kafka
 }


}
