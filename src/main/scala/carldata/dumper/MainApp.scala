package carldata.dumper

import java.util.Properties

import com.datastax.driver.core.Statement
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
  * Connect to all kafka topics, then for each message
  *  - Convert it to hydra-streams record
  *  - Serialize to Cassandra
  */
object MainApp {

  /** How long to wait for new batch of data. In milliseconds */
  val POLL_TIMEOUT = 100
  /** Data topic name */
  val DATA_TOPIC = "datatest"

  private val Log = LoggerFactory.getLogger("Dumper")

  case class Params(kafkaBroker: String, prefix: String)

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    val prefix = args.find(_.contains("--prefix=")).map(_.substring(9)).getOrElse("")
    Params(kafka, prefix)
  }

  /** Kafka configuration */
  def buildConfig(params: Params): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", params.kafkaBroker)
    props.put("group.id", "dumper")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }


  /** Extract only messages (skip keys) for given topic */
  def getTopicMessages(batch: ConsumerRecords[String, String], topic: String): Seq[String] =
    batch.records(topic).asScala.map(_.value()).toList


  /** Listen to Kafka topics and execute all processing pipelines */
  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val consumer = new KafkaConsumer[String, String](buildConfig(params))
    consumer.subscribe(List("data").asJava)
    while (true) {
      val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
      val dataStmt = DataProcessor.process(getTopicMessages(batch, DATA_TOPIC))
      if(dataStmt.forall(sendToCassandra)) {
        consumer.commitSync()
      }
    }
  }

  /**
    * Execute given statement on Cassandra driver
    * @return True if successful
    */
  def sendToCassandra(stmt: Statement): Boolean = {
    // Write serialization to the Cassandra here
    // Remember to wait till Cassandra confirms, that data was ingested before returning
    // status
    Log.error("Cassandra not implemented yet")
    false
  }
}
