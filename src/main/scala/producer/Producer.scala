package producer

import java.util.{Properties, UUID}

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import domain.User
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream

import org.apache.avro.io._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.io.Source

class KafkaProducer() {

  private val props = new Properties()

  props.put("metadata.broker.list", "localhost:9092")
  props.put("message.send.max.retries", "5")
  props.put("request.required.acks", "-1")
  props.put("serializer.class", "kafka.serializer.DefaultEncoder")
  props.put("client.id", UUID.randomUUID().toString())

  private val producer = new Producer[String, Array[Byte]](new ProducerConfig(props))

  //Read avro schema file and
  val schema: Schema = new Parser().parse(Source.fromURL(getClass.getResource("/schema.avsc")).mkString)

  def send(topic: String, users: List[User]): Unit = {
    val genericUser: GenericRecord = new GenericData.Record(schema)
    try {
      val queueMessages = users.map { user =>
        // Create avro generic record object
        //Put data in that generic record object
        genericUser.put("id", user.id)
        genericUser.put("name", user.name)
        genericUser.put("email", user.email.orNull)

        // Serialize generic record object into byte array
        val writer = new SpecificDatumWriter[GenericRecord](schema)
        val out = new ByteArrayOutputStream()
        val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
        writer.write(genericUser, encoder)
        encoder.flush()
        out.close()

        val serializedBytes: Array[Byte] = out.toByteArray()

        new KeyedMessage[String, Array[Byte]](topic, serializedBytes)
      }
      producer.send(queueMessages: _*)
    } catch {
      case ex: Exception =>
        println(ex.printStackTrace().toString)
        ex.printStackTrace()
    }
  }

}
