package carldata.dumper

import java.net.InetAddress
import java.util.Properties

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core._
import com.datastax.driver.mapping.{Mapper, MappingManager}
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
  /** Maximum number of records read by poll function */
  val MAX_POLL_RECORDS = 100
  /** Data topic name */
  val DATA_TOPIC = "data"
  /** Real time topic name */
  val REAL_TIME_TOPIC = "hydra-rt"
  /** Delete data topic */
  val DELETE_DATA_TOPIC = "delete-data"

  private val Log = LoggerFactory.getLogger(MainApp.getClass.getName)

  case class Params(kafkaBroker: String, prefix: String, keyspace: String, cassandraUrls: Seq[InetAddress],
                    cassandraPort: Int, user: String, pass: String, statsDHost: String)

  def stringArg(args: Array[String], key: String, default: String): String = {
    val name = "--" + key + "="
    args.find(_.contains(name)).map(_.substring(name.length)).getOrElse(default).trim
  }

  /** Command line parser */
  def parseArgs(args: Array[String]): Params = {
    val kafka = stringArg(args, "kafka", "localhost:9092")
    val prefix = stringArg(args, "prefix", "")
    val user = stringArg(args, "user", "")
    val pass = stringArg(args, "pass", "")
    val keyspace = stringArg(args, "keyspace", "production")
    val cassandraUrls = stringArg(args, "db", "localhost").split(",").map(InetAddress.getByName)
    val cassandraPort = stringArg(args, "dbPort", "9042").toInt
    val statsDHost = stringArg(args, "statsDHost", "none")
    Params(kafka, prefix, keyspace, cassandraUrls, cassandraPort, user, pass, statsDHost)
  }

  /** Kafka configuration */
  def buildConfig(brokers: String): Properties = {
    val strDeserializer = (new StringDeserializer).getClass.getName
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "dumper")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS.toString)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, strDeserializer)
    props
  }

  /** Init connection to the database */
  def initDB(params: Params): Session = {
    val builder = Cluster.builder()
      .addContactPoints(params.cassandraUrls.asJava)
      .withPort(params.cassandraPort)

    if (params.user != "" && params.pass != "") {
      builder.withCredentials(params.user, params.pass)
    }

    builder.build().connect(params.keyspace)
  }

  /** Extract only messages (skip keys) for given topic */
  def getTopicMessages(batch: ConsumerRecords[String, String], topic: String): Seq[String] =
    batch.records(topic).asScala.map(_.value()).toList


  /** Listen to Kafka topics and execute all processing pipelines */
  def main(args: Array[String]): Unit = {
    val params = parseArgs(args)
    StatsD.init("dumper", params.statsDHost)
    val session = initDB(params)
    //val manager = new MappingManager(session)
    Log.info("Application started")
    run(params.kafkaBroker, params.prefix, dbExecutor(session), session)
    Log.info("Application Stopped")
  }

  /**
    * Main processing loop:
    *  - Read batch of data from all topics
    *  - Split by channel. And for each channel
    *    - Create db statement
    *    - Execute statement on db
    */
  def run(kafkaBroker: String, prefix: String, dbExecute: Statement => Boolean, session: Session): Unit = {
    val kafkaConfig = buildConfig(kafkaBroker)
    val consumer = new KafkaConsumer[String, String](kafkaConfig)
    consumer.subscribe(List(DATA_TOPIC, REAL_TIME_TOPIC, DELETE_DATA_TOPIC).map(t => prefix + t).asJava)

    while (true) {
      try {
        val batch: ConsumerRecords[String, String] = consumer.poll(POLL_TIMEOUT)
        val records = getTopicMessages(batch, prefix + DATA_TOPIC)
        val dataStmt = DataProcessor.process(records)

        val realTimeMessages = getTopicMessages(batch, prefix + REAL_TIME_TOPIC)
        val realTimeDataStmt = RealTimeProcessor.process(realTimeMessages)

        //getting info about RealTimeJobs and channels to delete
        val deleteDataMessages = getTopicMessages(batch, prefix + DELETE_DATA_TOPIC)

        var deleteDataStmt = Seq[Statement]()
        if(deleteDataMessages.nonEmpty) {
          //val realTimeJobs = dbQueryExecute(QueryBuilder.select().from("real_time_jobs"))
          deleteDataStmt =  new DeleteDataProcessor(session).process(deleteDataMessages)
          //deleteDataStmt = DeleteDataProcessor.processDeleteDataMessages(deleteDataMessages, realTimeJobs).toSeq.flatten
        }

        if ((dataStmt ++ realTimeDataStmt ++ deleteDataStmt).forall(dbExecute)) {
          consumer.commitSync()
          StatsD.increment("data.out.count", records.size)
        }

//        if(deleteDataMessages.nonEmpty) {
//          val realTimeJobs = dbQueryExecute(QueryBuilder.select().from("real_time_jobs"))
//          deleteDataStmt = DeleteDataProcessor.processDeleteDataMessages(deleteDataMessages, realTimeJobs).toSeq.flatten
//        }
//
//        if ((dataStmt ++ realTimeDataStmt ++ deleteDataStmt).forall(dbExecute)) {
//          consumer.commitSync()
//          StatsD.increment("data.out.count", records.size)
//        }
      }
      catch {
        case e: CommitFailedException =>
          StatsD.increment("data.error.commit")
          Log.warn(e.toString)
        case e: Exception =>
          StatsD.increment("data.error")
          Log.error(e.toString)
      }
    }
    consumer.close()
  }

  /**
    * Execute given statement on Cassandra driver
    *
    * @return True if successful
    */

  def dbExecutor(session: Session): Statement => Boolean = { stmt =>
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

  def dbQueryExecutor(session: Session): Statement => Seq[RealTimeJobJava] = { stmt =>
    try {
      val manager = new MappingManager(session)
      val mapper: Mapper[RealTimeJobJava] = manager.mapper[RealTimeJobJava](classOf[RealTimeJobJava])
      val result = session.execute(stmt)
      mapper.map(result).asScala.toSeq
    }
    catch {
      case e: Exception =>
        Log.error(e.toString)
        Seq[RealTimeJobJava]()
    }
  }
}
