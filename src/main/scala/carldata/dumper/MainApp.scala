package carldata.dumper

import java.util.Properties
import java.util.logging.Logger

import com.datastax.driver.core.{Cluster, Session, Statement}
import com.timgroup.statsd.{NonBlockingStatsDClient, ServiceCheck, StatsDClient}
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerRecords, KafkaConsumer}

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

  private val Log = Logger.getLogger("Dumper")
  private var keepRunning: Boolean = true

  private val statsDCClient: StatsDClient = new NonBlockingStatsDClient(
    "dumper",
    "localhost",
    8125
  )

  val sc: ServiceCheck = ServiceCheck.builder.withName("service.check").withStatus(ServiceCheck.Status.OK).build

  case class Params(kafkaBroker: String, prefix: String, cassandraKeyspace: String, cassandraDB: String, user: String, pass: String) {
    val cassandraUrl: String = cassandraDB.split(":")(0)
    val cassandraPort: Int = cassandraDB.split(":")(1).toInt
  }

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    val prefix = args.find(_.contains("--prefix=")).map(_.substring(9)).getOrElse("")
    val user = args.find(_.contains("--user=")).map(_.substring(7)).getOrElse("")
    val pass = args.find(_.contains("--pass=")).map(_.substring(7)).getOrElse("")
    val cassandraKeyspace = args.find(_.contains("--keyspace=")).map(_.substring(11)).getOrElse("production")
    val cassandraDB = args.find(_.contains("--db=")).map(_.substring(5)).getOrElse("localhost:9042")
    Params(kafka, prefix, cassandraKeyspace, cassandraDB, user, pass)
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
    val builder = Cluster.builder()
      .addContactPoint(params.cassandraUrl)
      .withPort(params.cassandraPort)

    Log.info(params.toString)

    if (params.user != "" && params.pass != "") {
      builder.withCredentials(params.user, params.pass)
    }

    val session = builder.build().connect()
    session.execute("USE " + params.cassandraKeyspace)

    Log.info("Application started")
    run(params.kafkaBroker, params.prefix, initDB(session))
    Log.info("Application Stopped")
  }

  /**
    * Main processing loop:
    *  - Read batch of data from all topics
    *  - Split by channel. And for each channel
    *    - Create db statement
    *    - Execute statement on db
    */
  def run(kafkaBroker: String, prefix: String, dbExecute: Statement => Boolean): Unit = {
    statsDCClient.serviceCheck(sc)
    val kafkaConfig = buildConfig(kafkaBroker)
    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    consumer.subscribe(List(prefix + DATA_TOPIC).asJava)
    Log.info(consumer.toString)
    Log.info("keepRunning: " + keepRunning)

    while (keepRunning) {
      try {
        val startBatchProcessing = System.currentTimeMillis()
        Log.info("Poll timeout: " + POLL_TIMEOUT)
        val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
        //if (batch.count() > 0)
        Log.info("batch count: " + batch.count())
        val records = getTopicMessages(batch, prefix + DATA_TOPIC)
        if (records.nonEmpty)
          Log.info("Records length: " + records.length)
        val dataStmt = DataProcessor.process(records)
        if (dataStmt.forall(dbExecute)) {
          consumer.commitSync()
          val processingTime = System.currentTimeMillis() - startBatchProcessing
          val eventsCount = records.length
          if (processingTime > 0 && eventsCount > 0) {
            records.foreach(_ => statsDCClient.incrementCounter("events.passed"))
            Log.info("eps: " + (1000.0 * eventsCount) / processingTime.toFloat)
          }
        }
      }
      catch {
        case e: CommitFailedException => Log.warning(e.toString)
        case e: Exception => Log.warning(e.toString)
      }
    }
    consumer.close()
    statsDCClient.stop()
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
        case e: Exception =>
          Log.severe("Exception occurred: " + e.toString)
          false
      }
    }
  }
}
