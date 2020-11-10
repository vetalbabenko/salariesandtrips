import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


class KafkaProducerService(servers: String, topic: String) {
  private val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", servers)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  private val producer = new KafkaProducer[String, String](kafkaProducerProps)

  def writeFile(records: List[String]): Unit = {
    records.foreach { line =>
      producer.send(new ProducerRecord[String, String](topic, line))
    }
  }
}
