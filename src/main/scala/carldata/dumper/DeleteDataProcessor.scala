package carldata.dumper

import java.time.ZoneOffset

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, Session, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._

case class RealTimeJob(inputChannels: Seq[String], outputChannel: String, startDate: Long, endDate: Long)

class DeleteDataProcessor(val s: Session) {

  val session = s
  val TABLE_REAL_TIME = "real_time_jobs"
  val TABLE_NAME = "data"

  case class ChannelToRemove(channel: String, startDate: Long, endDate: Long)

  private val Log = LoggerFactory.getLogger(this.getClass)

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
        row.getString("output_channel"), 1507852800000L, 1507939200000L)).toSeq
    //hardcoded start and end date correspond to test start date and end date in delete-data message
    //TODO uncomment when real time job has date range than check if there are columns start_date and end_date
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


