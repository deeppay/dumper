package carldata.dumper

import java.time.{LocalDateTime, ZoneOffset}

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, Session, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._


class DeleteDataProcessor(val s: Session) {

  val session = s
  val TABLE_REAL_TIME = "real_time_jobs"
  val TABLE_NAME = "data"

  case class RealTimeJob(inputChannels: Seq[String], outputChannel: String, startDate: Long, endDate: Long)

  case class ChannelToRemove(channel: String, startDate: Long, endDate: Long)

  private val Log = LoggerFactory.getLogger(this.getClass)

  def processDeleteDataMessages(messages: Seq[String], realTimeJobs: Seq[RealTimeJobJava]): Option[Seq[Statement]] = {
    val deleteDataRecords = getDeleteRecords(messages)

    if (deleteDataRecords.nonEmpty) {
      var deleteDataStmt: Seq[Statement] = Seq[Statement]()

      deleteDataRecords.foreach(ddr => {
        val channelsToDelete = realTimeJobs.filter(rtj => rtj.input_channels.asScala.contains(ddr.channelId))
          .map(rtj => rtj.output_channel) ++ Seq(ddr.channelId)

        deleteDataStmt ++= process_old(channelsToDelete, ddr.startDate, ddr.endDate)
      })

      //println("delete data statements: ")
      //deleteDataStmt.foreach(b => b.asInstanceOf[BatchStatement].getStatements.asScala.foreach(s => println(s.toString)))
      Some(deleteDataStmt)
    }
    else
      None
  }

  def process_old(channels: Seq[String], startDate: LocalDateTime, endDate: LocalDateTime): Option[Statement] = {
    if (channels.isEmpty)
      None
    else {
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

  def process(messages: Seq[String]): Seq[Statement] = {
    val records = messages.flatMap(deserialize)
    if (records.isEmpty)
      Seq[Statement]()
    else {
      val realTimeJobs = getRealTimeJobs()
      records.map(r => buildDeleteStatements(r.channelId, r.startDate.toInstant(ZoneOffset.UTC).toEpochMilli,
        r.endDate.toInstant(ZoneOffset.UTC).toEpochMilli, realTimeJobs))
    }
  }

  def getRealTimeJobs(): Seq[RealTimeJob] = {

    session.execute(QueryBuilder.select().from(TABLE_REAL_TIME)).asScala
      .map(row => RealTimeJob(row.getList[String]("input_channels", classOf[String]).asScala,
        row.getString("output"), 0, 0)).toSeq
    //TODO uncomment when real time job has date range
    //    session.execute(QueryBuilder.select().from(TABLE_REAL_TIME)).asScala
    //      .map(row => RealTimeJob(row.getList[String]("input_channels", classOf[String]).asScala,
    //        row.getString("output"), row.getDate("start_date").getMillisSinceEpoch, row.getDate("end_date").getMillisSinceEpoch)).toSeq
  }

  def buildDeleteStatements(channelId: String, startDate: Long, endDate: Long, realTimeJobs: Seq[RealTimeJob]): Statement = {

    val channelsToRemove = realTimeJobs.filter(rtj => rtj.inputChannels.contains(channelId))
      .map(rtj => ChannelToRemove(rtj.outputChannel, rtj.startDate, rtj.endDate)) ++
      Seq(ChannelToRemove(channelId, startDate, endDate))

    val batch = new BatchStatement()

    channelsToRemove.foreach { c =>
      if( startDate <= c.endDate && endDate >= c.startDate) {
        val deleteStmt = QueryBuilder.delete().from(TABLE_NAME).where(QueryBuilder.eq("channel", c.channel))
          .and(QueryBuilder.gte("timestamp", if(startDate >= c.startDate) startDate else c.startDate))
          .and(QueryBuilder.lte("timestamp", if(endDate <= c.endDate) endDate else c.endDate))
        batch.add(deleteStmt)
      }
    }
    batch
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


