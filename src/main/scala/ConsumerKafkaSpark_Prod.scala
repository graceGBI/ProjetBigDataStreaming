import SparkBigData.getSparkStreamingContext
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object ConsumerKafkaSpark_Prod {
  def main(args: Array[String]): Unit = {

    val ssc : StreamingContext = getSparkStreamingContext(true, 2)

    val kk_consumer = getConsommateurKafka(ssc, Array("orderline"))

    kk_consumer.foreachRDD{

      rdd_kafka => {
        val offsets_kafka = rdd_kafka.asInstanceOf[HasOffsetRanges].offsetRanges
        val data_kafka = rdd_kafka.map(e => e.value())

        data_kafka.foreach{
          e => println(e)
        }

        kk_consumer.asInstanceOf[CanCommitOffsets].commitAsync(offsets_kafka)

      }

    }

    ssc.start()
    ssc.awaitTermination()

  }


  def getKafkaSparkConsumerParams () : Map[String, Object] = {
    val KafkaParam = Map(
      "bootstrap.servers" -> "localhost:9092",
      "group.id"  -> "gbi03",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    return KafkaParam

  }

  def getConsommateurKafka( StreamContext : StreamingContext, KafkaTopics : Array[String]) : InputDStream[ConsumerRecord[String, String]] = {

    val KafkaParametres = getKafkaSparkConsumerParams()
    val consommateurKafka = KafkaUtils.createDirectStream[String, String](
      StreamContext,
      PreferConsistent,
      Subscribe[String, String](KafkaTopics, KafkaParametres ))

    return consommateurKafka

  }
}
