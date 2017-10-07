package carldata.dumper

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._


/**
  * Connect to all kafka topics, then for each message
  *  - Convert it to hydra-streams record
  *  - Serialize to Cassandra
  */
object MainApp {

  val POLL_TIMEOUT = 100 // Milliseconds

  case class Params(kafkaBroker: String, prefix: String)

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    val prefix = args.find(_.contains("--prefix=")).map(_.substring(9)).getOrElse("")
    Params(kafka, prefix)
  }

  def buildConfig(params: Params): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", params.kafkaBroker)
    props.put("group.id", "dumper")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val consumer = new KafkaConsumer[String, String](buildConfig(params))
    consumer.subscribe(List("data").asJava)
    while (true) {
      val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
      val dataResult = processDataTopic(batch.records("data").asScala.toList)
      if(dataResult) {
        consumer.commitSync()
      }
    }
  }

  /** Returns True if data was fully processed */
  def processDataTopic(records: Seq[ConsumerRecord[String, String]]): Boolean = {
    records.foreach(println)
    true
  }
}
