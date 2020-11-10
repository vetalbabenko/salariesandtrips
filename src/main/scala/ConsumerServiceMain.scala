import scala.io.Source

//Entry point for application. Run kafka consumer with corresponding services.
object ConsumerServiceMain extends App {

  val hadoopService = new HadoopService("hdfs://172.27.1.5:8020")
  val igniteService = new IgniteService()

  val kafkaProducerService = new KafkaProducerService("172.17.0.1:9092", "user")
  val consumerService = new KafkaConsumerService("172.17.0.1:9092", List("user"), igniteService, hadoopService)

  kafkaProducerService.writeFile(Source.fromResource("sample.csv").getLines().toList)
  consumerService.consume()


}
