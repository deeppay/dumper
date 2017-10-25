package carldata.dumper

import java.net.InetAddress
import java.util.Properties

import org.slf4j.LoggerFactory
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

  private val Log = LoggerFactory.getLogger(MainApp.getClass)
  private var keepRunning: Boolean = true

  val sc: ServiceCheck = ServiceCheck.builder.withName("service.check").withStatus(ServiceCheck.Status.OK).build

  case class Params(kafkaBroker: String, prefix: String, cassandraKeyspace: String, cassandraUrls: Seq[InetAddress],
                    cassandraPort: Int, user: String, pass: String, statSDHost: String) {
  }

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = args.find(_.contains("--kafka=")).map(_.substring(8)).getOrElse("localhost:9092")
    val prefix = args.find(_.contains("--prefix=")).map(_.substring(9)).getOrElse("")
    val user = args.find(_.contains("--user=")).map(_.substring(7)).getOrElse("")
    val pass = args.find(_.contains("--pass=")).map(_.substring(7)).getOrElse("")
    val cassandraKeyspace = args.find(_.contains("--keyspace=")).map(_.substring(11)).getOrElse("production")
    val cassandraUrls = args.find(_.contains("--db=")).map(_.substring(5)).getOrElse("localhost").split(",")
      .map(address => InetAddress.getByName(address))
    val cassandraPort = args.find(_.contains("--dbPort=")).map(_.substring(9)).getOrElse("9042").toInt
    val statSDHost = args.find(_.contains("--statSDHost=")).map(_.substring(13)).getOrElse("none")
    Params(kafka, prefix, cassandraKeyspace, cassandraUrls, cassandraPort, user, pass, statSDHost)
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
    val statsDCClient: Option[StatsDClient] =
      if (params.statSDHost == "none") None
      else {
        Some(
          new NonBlockingStatsDClient(
            "dumper",
            params.statSDHost,
            8125
          ))
      }
    val builder = Cluster.builder()
      .addContactPoints(params.cassandraUrls.asJava)
      .withPort(params.cassandraPort)

    if (params.user != "" && params.pass != "") {
      builder.withCredentials(params.user, params.pass)
    }

    val session = builder.build().connect()
    session.execute("USE " + params.cassandraKeyspace)

    Log.info("Application started")
    run(params.kafkaBroker, params.prefix, statsDCClient, initDB(session))
    Log.info("Application Stopped")
  }

  /**
    * Main processing loop:
    *  - Read batch of data from all topics
    *  - Split by channel. And for each channel
    *    - Create db statement
    *    - Execute statement on db
    */
  def run(kafkaBroker: String, prefix: String, statsDClient: Option[StatsDClient], dbExecute: Statement => Boolean): Unit = {
    statsDClient.foreach(_.serviceCheck(sc))
    val kafkaConfig = buildConfig(kafkaBroker)
    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    consumer.subscribe(List(prefix + DATA_TOPIC).asJava)

    while (keepRunning) {
      try {
        val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
        val records = getTopicMessages(batch, prefix + DATA_TOPIC)
        val dataStmt = DataProcessor.process(records)
        if (dataStmt.forall(dbExecute)) {
          consumer.commitSync()
          statsDClient.foreach(sdc => records.foreach(_ => sdc.incrementCounter("events.passed")))
        }
      }
      catch {
        case e: CommitFailedException => Log.warn(e.toString)
        case e: Exception => Log.error(e.toString)
      }
    }
    consumer.close()
    statsDClient.foreach(_.stop())
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
          Log.error("Exception occurred: " + e.toString)
          false
      }
    }
  }
}
