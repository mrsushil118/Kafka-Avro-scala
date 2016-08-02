name := "kafka-avro"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
      "org.apache.avro" % "avro" % "1.7.7",
      "org.apache.kafka" % "kafka_2.11" % "0.10.0.0",
     // "io.confluent" % "kafka-avro-serializer" % "3.0.0",
      "ch.qos.logback" %  "logback-classic" % "1.1.7"
)

/*resolvers ++= Seq(
      Resolver.sonatypeRepo("public"),
      "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)*/
