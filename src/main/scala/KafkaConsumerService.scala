import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

class KafkaConsumerService(servers: String, topics: List[String], igniteService: IgniteService) {
  val properties = new Properties()
  properties.put("bootstrap.servers", servers)
  properties.put("key.deserializer", classOf[StringDeserializer])
  properties.put("value.deserializer", classOf[StringDeserializer])
  properties.put("group.id", "group-test")
  properties.put("auto.commit.interval.ms", "1000")
  properties.put("auto.offset.reset", "earliest")

  val kafkaConsumer = new KafkaConsumer[String, String](properties)
  kafkaConsumer.subscribe(topics.asJavaCollection)

  def consume(): Unit = {
    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      results.foreach { record =>
        println(s"Received record ${record.value()}")
        if (record.value() == "run-agg")
          igniteService.runAggregation()
        else
          igniteService.add(record.value())
      }
    }
  }
}
