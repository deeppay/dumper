package carldata.dumper

import java.util.Properties

import com.datastax.driver.core.{Cluster, Session, Statement}
import com.timgroup.statsd.{NonBlockingStatsDClient, ServiceCheck, StatsDClient}
import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
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

  private val Log = LoggerFactory.getLogger(MainApp.getClass)
  private var keepRunning: Boolean = true

  val sc: ServiceCheck = ServiceCheck.builder.withName("service.check").withStatus(ServiceCheck.Status.OK).build

  case class Params(kafkaBroker: String, prefix: String, cassandraKeyspace: String, cassandraDB: String, user: String, pass: String, statSDHost: String) {
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
    val statSDHost = args.find(_.contains("--statSDHost=")).map(_.substring(13)).getOrElse("none")
    Params(kafka, prefix, cassandraKeyspace, cassandraDB, user, pass, statSDHost)
  }

  /** Kafka configuration */
  def buildConfig(brokers: String): Properties = {
    val strDeserializer = (new StringDeserializer).getClass.getName
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dumper")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100.toString)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props
  }

  /** StatsD configuration */
  def initStatsD(host: String): Option[StatsDClient] = {
    if (host == "none") None
    else Some( new NonBlockingStatsDClient("dumper", host, 8125 ))
  }


  /** Extract only messages (skip keys) for given topic */
  def getTopicMessages(batch: ConsumerRecords[String, String], topic: String): Seq[String] =
    batch.records(topic).asScala.map(_.value()).toList


  /** Listen to Kafka topics and execute all processing pipelines */
  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    val statsDCClient = initStatsD(params.statSDHost)
    val builder = Cluster.builder()
      .addContactPoint(params.cassandraUrl)
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
          Log.error(e.toString)
          false
      }
    }
  }
}
