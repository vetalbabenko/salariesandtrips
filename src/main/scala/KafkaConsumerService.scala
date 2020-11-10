import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.jdk.CollectionConverters._

// Simple kafka consumer, which consumes records from topic, add them to ignite cache and run aggregation
// if corresponding message sent

class KafkaConsumerService(servers: String, topics: List[String], igniteService: IgniteService, hadoopService: Hadoop) {
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
    val results = kafkaConsumer.poll(2000).asScala
    results.foreach { record =>
      println(s"Received record ${record.value()}")
      igniteService.add(record.value())
    }
    val aggregatedData = igniteService.runAggregation()
    hadoopService.save(aggregatedData)
  }
}
