name := "SalariesTrips"

version := "0.1"

scalaVersion := "2.13.3"


libraryDependencies ++= Seq(
  "org.apache.ignite" % "ignite-core" % "2.8.1",
  "org.apache.ignite" % "ignite-indexing" % "2.8.1",
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "org.apache.logging.log4j" % "log4j" % "2.8.2" pomOnly(),
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % "2.5.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0",
  "org.apache.hadoop" % "hadoop-common" % "3.3.0",
  "org.apache.hadoop" % "hadoop-client" % "3.3.0",
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "org.mockito" %% "mockito-scala" % "1.15.0" % Test

)