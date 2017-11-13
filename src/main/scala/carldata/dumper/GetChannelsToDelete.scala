package carldata.dumper

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

object GetChannelsToDelete {

  val TABLE_NAME = "real_time_jobs"

  private val Log = LoggerFactory.getLogger(this.getClass)

  def getAllRealTimeJobs(): Statement = {
    QueryBuilder.select().from(TABLE_NAME)
  }

  def getDeleteRecords(messages: Seq[String]): Seq[DeleteDataRecord] = {
    messages.flatMap(deserialize)
  }

  def deserialize(rec: String): Option[DeleteDataRecord] = {
    try {
      Some(JsonParser(rec).convertTo[DeleteDataRecord])
    } catch {
      case _: ParsingException =>
        Log.error("Can't deserialize delete data record: " + rec)
        None
    }

  }
}

