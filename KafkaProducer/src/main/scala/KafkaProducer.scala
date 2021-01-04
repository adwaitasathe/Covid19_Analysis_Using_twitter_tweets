import org.apache.kafka.clients.producer._
import java.util.Properties
import java.util.ArrayList

import org.slf4j.LoggerFactory
import twitter4j.JSONObject

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success, Try}

object KafkaProducer extends App {

  println("Starting Producer Application" )

  val propertiesFile = getClass.getResource("application.properties")
  val properties: Properties = new Properties()

  if (propertiesFile != null) {
    val source = Source.fromURL(propertiesFile)
    properties.load(source.bufferedReader())
    println("properties file loaded" )
  }
  else {
    println("properties file cannot be loaded at path ")
  }

  if (args.length == 0) {
    println("Please specify the argument")
    System.exit(1)
  }

  val numberOfTweets = Integer.parseInt(args(0))

  val bootStrapServer = properties.getProperty("bootStrapServer")
  val acks            = properties.getProperty("acks")
  val requestTimeout  = properties.getProperty("timeoutTime")
  val topic           = properties.getProperty("topic")
  val hashtag         = properties.getProperty("hashtag")
  val consumerKey     = properties.getProperty("consumerKey")
  val consumerSecret  = properties.getProperty("consumerSecret")
  val token           = properties.getProperty("token")
  val tokenSecret     = properties.getProperty("tokenSecret")


  val m = Map("bootstrap.servers"->bootStrapServer, "acks"->acks, "key.serializer"->"org.apache.kafka.common.serialization.StringSerializer", "value.serializer"->"org.apache.kafka.common.serialization.StringSerializer", "request.timeout.ms"->requestTimeout)

  val kfk = Kafka(m)
  val account = Account(consumerKey, consumerSecret, token,tokenSecret)
  val twitter = Twitters(account)

  twitter.searchN(numberOfTweets, hashtag)

  val ResultToJson: ListBuffer[JSONObject] = twitter.toJson

  kfk.sendList(topic, ResultToJson.toList)
  kfk.close

  println("Closing Application" )

}
