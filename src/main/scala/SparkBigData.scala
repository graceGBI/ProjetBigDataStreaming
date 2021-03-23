import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.streaming._

object SparkBigData {

  var ss : SparkSession = null;
  var spConf : SparkConf = null;
  /**
   * fonction d'initialisation et instanciation d'une session spark
   * @param Env : variable qui indique l'env. sur laquelle l'appli spark est déployée
   *            Si Env= true alors l'appli est déployée en local, sinon en production
   */
  def maSessionSpark(Env : Boolean = true): SparkSession ={
    //en local on passe le nom de notre bin Hadoop
    // dans un cluster, la config de l'env est différente
    if(Env == true){
      System.setProperty("hadoop.home.dir" ,"C:/hadoop")
      ss= SparkSession.builder()
        .master("local[*]")
        .config("spark.sql.crossJoin.enabled","true")
        .enableHiveSupport()
        .getOrCreate()
    }else{
      ss = SparkSession.builder()
        .appName("My first spark appli")
        .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.crossJoin.enabled","true")
        .enableHiveSupport()
        .getOrCreate()
    }
    return ss
  }

  /**
   *
   * @param env : variable qui indique l'env. sur laquelle l'appli spark est déployée
   *            Si Env= true alors l'appli est déployée en local, sinon en production
   * @param duree_batch : c'est le SparkStreamingBactchDuration la durée du micro-batch
   * @return : la fonction renvoie une instance du contexte streaming
   */
  def getSparkStreamingContext(env : Boolean = true, duree_batch : Int) : StreamingContext ={
    if(env){
      spConf = new SparkConf().setMaster("localHost[2]")
        .setAppName("My first streaming appli")
    }else{
      spConf = new SparkConf().setAppName("My first streaming appli")
    }

    val ssc : StreamingContext = new StreamingContext(spConf, Seconds(duree_batch))

    return ssc
  }




}
