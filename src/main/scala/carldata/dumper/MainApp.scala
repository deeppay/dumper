package carldata.dumper

import java.util.Properties

import com.datastax.driver.core.{Cluster, Session, Statement}
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRecords, KafkaConsumer}
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
  val DATA_TOPIC = "data"

  private val Log = LoggerFactory.getLogger("Dumper")
  private var keepRunning: Boolean = true

  case class Params(kafkaBroker: String, prefix: String, cassandraKeyspace: String, cassandraDB: String) {
    val cassandraUrl = cassandraDB.split(":")(0)
    val cassandraPort = cassandraDB.split(":")(1).toInt
  }

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    val prefix = args.find(_.contains("--prefix=")).map(_.substring(9)).getOrElse("")
    val cassandraKeyspace = args.find(_.contains("--keyspace=")).map(_.substring(11)).getOrElse("production")
    val cassandraDB = args.find(_.contains("--db=")).map(_.substring(5)).getOrElse("localhost:9042")
    Params(kafka, prefix, cassandraKeyspace, cassandraDB)
  }

  /** Kafka configuration */
  def buildConfig(brokers: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
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
    val session = Cluster.builder()
      .addContactPoint(params.cassandraUrl)
      .withPort(params.cassandraPort)
      .build()
      .connect()
    session.execute("USE " + params.cassandraKeyspace)

    run(params.kafkaBroker, params.prefix, initDB(session))
  }

  /**
    * Main processing loop:
    *  - Read batch of data from all topics
    *  - Split by channel. And for each channel
    *    - Create db statement
    *    - Execute statement on db
    */
  def run(kafkaBroker: String, prefix: String, dbExecute: Statement => Boolean): Unit = {
    val kafkaConfig = buildConfig(kafkaBroker)
    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    consumer.subscribe(List(prefix + DATA_TOPIC).asJava)

    while (keepRunning) {
      try {
        val startBatchProcessing = System.currentTimeMillis()
        val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
        val dataStmt = DataProcessor.process(getTopicMessages(batch, prefix + DATA_TOPIC))
        if (dataStmt.forall(dbExecute)) {
          consumer.commitSync()
          val processingTime = System.currentTimeMillis() - startBatchProcessing
          val eventsCount = dataStmt.toSeq.length
          Log.info("eps: " +  1000/(processingTime/eventsCount))
        }
      }
      catch {
        case e: CommitFailedException => Log.warn(e.toString())
      }
    }
    consumer.close()
  }

  /** Stop processing and exit application */
  def stop(): Unit = {
    keepRunning = false
  }

  /**
    * Execute given statement on Cassandra driver
    *
    * @return True if successful
    */

  def initDB(session: Session): Statement => Boolean = {
    stmt => {
      try {
        session.execute(stmt)
        true
      }
      catch {
        case e: Exception => {
          Log.error("Exception occurred: " + e.toString())
          false
        }
      }
    }
  }
}
