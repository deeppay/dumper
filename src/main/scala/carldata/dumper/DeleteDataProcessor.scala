package carldata.dumper

import java.time.{LocalDateTime, ZoneOffset}

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException
import scala.collection.JavaConverters._

object DeleteDataProcessor {

  val TABLE_NAME = "data"

  private val Log = LoggerFactory.getLogger(this.getClass)

  def processDeleteDataMessages(messages: Seq[String], realTimeJobs: Seq[RealTimeJob] ): Option[Seq[Statement]] = {
    val deleteDataRecords = getDeleteRecords(messages)

    if(deleteDataRecords.nonEmpty){
      var deleteDataStmt: Seq[Statement] = Seq[Statement]()

      deleteDataRecords.foreach(ddr => {
        val channelsToDelete = realTimeJobs.filter(rtj => rtj.input_channels.asScala.contains(ddr.channelId))
                .map(rtj => rtj.output_channel) ++ Seq(ddr.channelId)

        deleteDataStmt ++= process(channelsToDelete, ddr.startDate, ddr.endDate)
      })

      //println("delete data statements: ")
      //deleteDataStmt.foreach(b => b.asInstanceOf[BatchStatement].getStatements.asScala.foreach(s => println(s.toString)))
      Some(deleteDataStmt)
    }
    else
      None
  }

  def process(channels: Seq[String], startDate: LocalDateTime, endDate: LocalDateTime): Option[Statement] ={
    if(channels.isEmpty)
      None
    else{
      val batch = new BatchStatement()

      channels.foreach { c =>
        val deleteStmt = QueryBuilder.delete().from(TABLE_NAME).where(QueryBuilder.eq("channel", c))
          .and(QueryBuilder.gte("timestamp", startDate.toInstant(ZoneOffset.UTC).toEpochMilli))
          .and(QueryBuilder.lte("timestamp", endDate.toInstant(ZoneOffset.UTC).toEpochMilli))
        batch.add(deleteStmt)
      }
      Some(batch)
    }
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


