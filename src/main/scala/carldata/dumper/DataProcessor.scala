package carldata.dumper

import java.time.ZoneOffset

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

/**
  * Process data topic
  */
object DataProcessor {

  val TABLE_NAME = "data"

  private val Log = LoggerFactory.getLogger(DataProcessor.getClass.getName)

  /** Convert data records into SQL Cassandra command */
  def process(messages: Seq[String]): Option[Statement] = {
    val records = messages.flatMap(deserialize)
    if (records.isEmpty) None
    else {
      val batch = new BatchStatement()
      records.foreach { m =>
        val insert = QueryBuilder.insertInto(TABLE_NAME)
          .value("channel", m.channelId)
          .value("timestamp", m.timestamp.toInstant(ZoneOffset.UTC).toEpochMilli)
          .value("value", m.value)
        batch.add(insert)
      }
      Some(batch)
    }
  }

  /** Convert from json with exception handling */
  def deserialize(rec: String): Option[DataRecord] = {
    try {
      Some(JsonParser(rec).convertTo[DataRecord])
    } catch {
      case _: ParsingException =>
        StatsD.increment("data.error.parser")
        Log.error("Can't deserialize data record: " + rec)
        None
    }
  }
}
