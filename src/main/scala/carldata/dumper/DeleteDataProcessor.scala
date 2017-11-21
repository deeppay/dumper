package carldata.dumper

import java.time.{LocalDateTime, ZoneOffset}

import carldata.dumper.DeleteDataProcessor.RealTimeInfo
import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Session, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._

object DeleteDataProcessor {

  case class RealTimeInfo(inputChannels: Seq[String], outputChannel: String, startDate: Long, endDate: Long)

}

class DeleteDataProcessor(session: Session) {

  private val Log = LoggerFactory.getLogger(this.getClass)

  val TABLE_REAL_TIME = "real_time_jobs"
  val TABLE_NAME = "data"


  case class ChannelRange(channel: String, startDate: Long, endDate: Long)

  def process(messages: Seq[String]): Seq[Statement] = {
    val realTimeJobs = getRealTimeJobs
    processMessages(messages, realTimeJobs)
  }

  def processMessages(messages: Seq[String], realTimeJobs: List[RealTimeInfo]): Seq[Statement] = {
    val result = messages.flatMap(deserialize)
      .flatMap { dr =>
        ChannelRange(dr.channelId, asMillis(dr.startDate), asMillis(dr.endDate)) ::
          channelsToRemove(dr.channelId, realTimeJobs)
            .map(ctr => trimChannelRange(ctr, asMillis(dr.startDate), asMillis(dr.endDate)))
            .filter(ctr => ctr.startDate <= ctr.endDate)
      }.map(buildDeleteStatement)
    if (result.nonEmpty)
      StatsD.increment("delete_data", messages.size)
    result
  }

  def getRealTimeJobs: List[RealTimeInfo] = {
    session.execute(QueryBuilder.select().from(TABLE_REAL_TIME)).asScala
      .map(row => RealTimeInfo(row.getList[String]("input_channels", classOf[String]).asScala,
        row.getString("output_channel"), row.getTimestamp("start_date").getTime, row.getTimestamp("end_date").getTime)).toList
  }

  def channelsToRemove(channelId: String, realTimeJobs: List[RealTimeInfo]): List[ChannelRange] = {
    realTimeJobs.filter(rtj => rtj.inputChannels.contains(channelId))
      .map(rtj => ChannelRange(rtj.outputChannel, rtj.startDate, rtj.endDate))
  }

  def trimChannelRange(channelRange: ChannelRange, startDate: Long, endDate: Long): ChannelRange = {
    ChannelRange(channelRange.channel, Math.max(startDate, channelRange.startDate),
      Math.min(endDate, channelRange.endDate))
  }

  def asMillis(date: LocalDateTime): Long = {
    date.toInstant(ZoneOffset.UTC).toEpochMilli
  }

  def buildDeleteStatement(channelRange: ChannelRange): Statement = {
    QueryBuilder.delete().from(TABLE_NAME).where(QueryBuilder.eq("channel", channelRange.channel))
      .and(QueryBuilder.gte("timestamp", channelRange.startDate))
      .and(QueryBuilder.lte("timestamp", channelRange.endDate))
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


