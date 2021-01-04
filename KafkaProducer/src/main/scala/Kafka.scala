import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

case class Kafka() {
  
  val prop: Properties = new Properties
  lazy val producer: KafkaProducer[String, String] =  new KafkaProducer[String, String](prop)

  def sendList[T](topic: String, list: List[T]): Unit = {
    for (l <- list) {
      //println(s"send --> ${l.toString} to $topic")
      val rmd: RecordMetadata = producer.send(new ProducerRecord[String, String](topic, l.toString)).get()
     // println(rmd.toString)
     // Thread.sleep(500)
    }
  }

  def close = producer.close
}

object Kafka {
  def apply(m: Map[String, String]): Kafka = new Kafka() {
    m.foreach(p => prop.setProperty(p._1, p._2))
  }
}
