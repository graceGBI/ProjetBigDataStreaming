import KafkaStreaming.{getKafkaProducerParams, getProducerKafka}
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.httpclient.auth._
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.{StatusesFilterEndpoint, StatusesSampleEndpoint}
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.{FilterQuery, StallWarning, Status, StatusDeletionNotice, StatusListener, TwitterStreamFactory}
import twitter4j.conf.ConfigurationBuilder
import twitter4j.auth._

import java.util.Collections
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}
import scala.collection.JavaConverters.seqAsJavaListConverter;


class TwitterKafkaStreaming {

  private var trace_client_streaming : Logger = LogManager.getLogger("Log_Console")


  private def  twitterOAuthConf (CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String) : ConfigurationBuilder =  {

    val twitterConfig = new ConfigurationBuilder()
    twitterConfig
      .setJSONStoreEnabled(true)
      .setDebugEnabled(true)
      .setOAuthConsumerKey(CONSUMER_KEY)
      .setOAuthConsumerSecret(CONSUMER_SECRET)
      .setOAuthAccessToken(ACCESS_TOKEN)
      .setOAuthAccessTokenSecret(TOKEN_SECRET)

    return twitterConfig

  }
  /**
   * Ce client est un client Hosebird. Il permet de collecter les tweets contenant une liste d'hashtag et de les publier
   * en temps réel dans un ou plusieurs topics kafka
   * @param CONSUMER_KEY : la clé du consommateur pour l'authentification OAuth
   * @param CONSUMER_SECRET : le secret du consommateur pour l'authentification OAuth
   * @param ACCESS_TOKEN : le token d'accès pour l'authentification OAuth
   * @param TOKEN_SECRET : le token secret pour l'authentification OAuth
   * @param liste_hashtags : la liste des hastags de tweets dont on souhaite collecter
   * @param kafkaBootStrapServers : la liste d'adresses IP (et leur port) des agents du cluster Kafka
   * @param topic : le(s) topic(s) dans le(s)quel(s) stocker les tweets collectés
   */
  def producerTwitterKafkaHbc(CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String,
                              liste_hashtags : String, kafkaBootStrapServers : String, topic : String): Unit = {

    val queue : BlockingQueue[String] = new LinkedBlockingQueue[String](10000)
    val auth : Authentication = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET)

    val endp : StatusesFilterEndpoint = new StatusesFilterEndpoint()
    endp.trackTerms(Collections.singletonList(liste_hashtags)) // les filtres sont dans une collection
    endp.trackTerms(List(liste_hashtags).asJava)   //Collections.singletonList

    //création du client
    val constructeur_hbc : ClientBuilder = new ClientBuilder()
      .hosts(Constants.STREAM_HOST)
      .authentication(auth)
      .gzipEnabled(true) //compression des messages
      .endpoint(endp)
      .processor(new StringDelimitedProcessor(queue))

    val client_hbc : Client = constructeur_hbc.build()

    try {
      client_hbc.connect() //établissement de la connexion

      while (!client_hbc.isDone) {
        //recuperation de toutes les données val message : String = queue.take()
        val tweets : String = queue.poll(15, TimeUnit.SECONDS) //recupération par fenetrage de 15 seconds
        getProducerKafka(kafkaBootStrapServers, topic, tweets)     // intégration avec notre producer Kafka
        println( "message Twitter : " + tweets)
      }
    } catch {// classe d'exception récupérer dans la doc de twitter hbc
        case ex : InterruptedException => trace_client_streaming.error("le client Twitter HBC a été interrompu à cause de cette erreur : " + ex.printStackTrace())
    } finally {
      client_hbc.stop()
      getProducerKafka(kafkaBootStrapServers, topic, "").close()

    }
  }

  /**
   * Ce client Twitter4J récupère les données streaming de Twitter. Il est un peu différent du client HBC, car il est plus vaste et plus complet
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param requete
   * @param kafkaBootStrapServers
   * @param topic
   */
  def producerTwitter4JKafka (CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String, requete : String,
                              kafkaBootStrapServers : String, topic : String) : Unit = {

    val queue : BlockingQueue[Status] = new LinkedBlockingQueue[Status](10000) // pour stocker les data

    //le client TwitterStream de Twitter4J
    val twitterStream  = new TwitterStreamFactory(twitterOAuthConf(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET).build()).getInstance()

    //L'instance du twitterStream se connecte au flux streaming des tweets et les envoie à tout client qui lui est en écoute.
    // Ainsi, pour pouvoir travailler avec les tweets, il faut implémenter un "écouteur de messages" (StatusListener) et le connecter au client TwitterStream.
    //
    //Implémenter un écouteur de messages revient a implémenter 6 types d'événements ce qui est différent du client hbc qui était directement en écoute:
    //- OnStatus : l'événement qui définit la récupération d'un tweet (statuts)
    //- OnDeletionNotice : notice de suppression
    //- onTrackLimitationNotice
    //- onException
    //- onScrubGeo
    //- onStallWarning
    val listener : StatusListener = new StatusListener {

      override def onStatus(status: Status): Unit = {
        trace_client_streaming.info("événement d'ajout de tweet détecté. Tweet complet : " + status.getText)
        queue.put(status) // en cas d'erreur on a le message dans la queue
        getProducerKafka(kafkaBootStrapServers, topic, status.getText)   // 1ère méthode

        // 2 ème méthode
        /*
        while(true){
          val tweet : Status = queue.poll(15, TimeUnit.SECONDS)
          if(tweet != null) {
            getProducerKafka(kafkaBootStrapServers, topic, tweet.getText)
          }
        }*/
      }

      override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}

      override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}

      override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}

      override def onStallWarning(warning: StallWarning): Unit = {}

      override def onException(ex: Exception): Unit = {
        trace_client_streaming.error("Erreur généré par Twitter : " + ex.printStackTrace())
      }
    }
    twitterStream.addListener(listener)
    //twitterStream.sample()       //déclenche la réception de tous les twests un milliers par seconde

    val query = new FilterQuery().track(requete)
    twitterStream.filter(query)         // filtre des données

    getProducerKafka(kafkaBootStrapServers, "", "").close()
    twitterStream.shutdown()
  }

  /**
   *  Client Spark Streaming Twitter Kafka. Ce client Spark Streaming se connecte à Twitter et publie les infos dans Kafka via un Producer Kafka
   * @param CONSUMER_KEY
   * @param CONSUMER_SECRET
   * @param ACCESS_TOKEN
   * @param TOKEN_SECRET
   * @param filtre
   * @param kafkaBootStrapServers
   * @param topic
   */
  def ProducerTwitterKafkaSpark (CONSUMER_KEY : String, CONSUMER_SECRET : String, ACCESS_TOKEN : String, TOKEN_SECRET : String, filtre : Array[String],
                                 kafkaBootStrapServers : String, topic : String) : Unit = {

    // authentification : instancier une nouvel authentification OAuth
    val auth0 = new OAuthAuthorization(twitterOAuthConf(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, TOKEN_SECRET).build())

    //le client Spark Streaming Twitter
    //Som est une spécification de scala qui permet de définir un paramètre optionel
    val client_Streaming_Twitter = TwitterUtils.createStream(SparkBigData.getSparkStreamingContext(true, 15), Some(auth0), filtre)
    //micro batch de 15 secondes des RDD/ le workers de 15 secondes

    //Manipulation des données

    val tweetsMsg = client_Streaming_Twitter.flatMap(statuts => (statuts.getText()))
    val tweetsCompets = client_Streaming_Twitter.flatMap(statuts => (statuts.getText() ++ statuts.getContributors() ++ statuts.getLang()))
    val tweetsFR = client_Streaming_Twitter.filter(status => status.getLang() == "fr")
    val hashtags = client_Streaming_Twitter.flatMap(statuts => statuts.getText().split(" ").filter(statuts => statuts.startsWith("#")))
    val hashtagsFR = tweetsFR.flatMap(statuts => statuts.getText().split(" ").filter(statuts => statuts.startsWith("#"))) //hashtags français
    val hashtagsCount = hashtagsFR.window(Minutes(3)) //compter ls hashtag français toutes les 3 min c'est du féneutrage

    // ATTENTION à cette erreur !!! getProducerKafka(kafkaBootStrapServers, topic, tweetsmsg.toString())

    // Par défaut les worker ou RDD sont réinitialisés afin d'éviter la surcharge applicatif.
    //Faire attention à ne pas exécuter la connexion au niveau du RDD
    //RDD : pas de contrôle dessus, théoriquement ils peuvents être vide à chaque D-Stream il y a des RDD. Ils ne sont pas créer au moment des données
    //mais pour chaque lot, ce sont des abstraction des lots de données en mémoire

    //D-Stream
    tweetsMsg.foreachRDD { // remplaçons les paranthèse pas des crochets pour effectuer des actions
      (tweetsRDD, temps) =>
        if (!tweetsRDD.isEmpty()) { //RDD
          tweetsRDD.foreachPartition { //partition de chaque RDD
            partitionOfTweets =>
              val prodcucer_kafka = new KafkaProducer[String, String](getKafkaProducerParams(kafkaBootStrapServers))
              partitionOfTweets.foreach { // pas d'instanciation à la lecture de chaque tweet pour ne pas surcharger l'application
                tweetEvent =>
                  val record_publish = new ProducerRecord[String, String](topic, tweetEvent.toString) // création de l'enregistrement
                  prodcucer_kafka.send(record_publish)

              }
              prodcucer_kafka.close()
          }
        }
    }
    // Le premier est plus recommandé car l'instanciation se fait en amont
    try {
      tweetsCompets.foreachRDD {
        tweetRDD =>
          if (!tweetRDD.isEmpty()) {
            tweetRDD.foreachPartition {
              tweetPartition =>
                tweetPartition.foreach {
                  tweets => getProducerKafka(kafkaBootStrapServers, topic, tweets.toString)
                }
            }
            getProducerKafka(kafkaBootStrapServers, "", "").close()
          }
      }
    }catch{
      case ex : Exception => trace_client_streaming.error(ex.printStackTrace())
    }

    //demarrer le contexte spark
    SparkBigData.getSparkStreamingContext(true, 15).start()
    //il attend que le micro bacth finisse avant d'en démarrer un autre
    SparkBigData.getSparkStreamingContext(true, 15).awaitTermination()

    //Pour arrêter le batch, il faut appeler la fonction stop SparkBigData.getSparkStreamingContext(true, 15).stop()

  }

}
