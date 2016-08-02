package demoApp

import domain.User
import producer. KafkaProducer

object ProducerApp extends App {

  private val topic = "demo-topic"

  val producer = new KafkaProducer()

  val user1 = User(1, "Sushil Singh", None)
  val user2 = User(2, "Satendra Kumar Yadav", Some("satendra@knoldus.com"))

  producer.send(topic, List(user1, user2))

}