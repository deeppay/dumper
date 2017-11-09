package carldata.dumper

import java.nio.ByteBuffer

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, ProtocolVersion, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._

object GetChannelsToDelete {

  val TABLE_NAME = "real_time_jobs"

  private val Log = LoggerFactory.getLogger(this.getClass)

  def getAllRealTimeJobs() : Statement = {
     QueryBuilder.select().from(TABLE_NAME)
  }

  def getDeleteRecords(messages: Seq[String]) : Option[Seq[DeleteDataRecord]] = {
    val records = messages.flatMap(deserialize)
    if(records.isEmpty) None
    else Some(records)
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

