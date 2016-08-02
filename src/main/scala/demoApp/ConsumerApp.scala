package demoApp

import consumer.KafkaConsumer

object ConsumerApp extends App {

  val consumer = new KafkaConsumer()

  while (true) {
    consumer.read() match {
      case Some(message) =>
        println("Got message: " + message)

        Thread.sleep(100)
      case _ =>
        println("Queue is empty.......................  ")
        // wait for 2 second
        Thread.sleep(2 * 1000)
    }
  }

}