package carldata.dumper

import java.time.ZoneOffset

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob, RemoveRealTimeJob}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{BatchStatement, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

import scala.collection.JavaConverters._

object RealTimeProcessor {

  val TABLE_NAME = "real_time_jobs"

  private val Log = LoggerFactory.getLogger(this.getClass)

  def process(messages: Seq[String]): Option[Statement] = {
    StatsD.increment("rtj",messages.size)
    val records = messages.flatMap(deserialize)
    if (records.isEmpty) None
    else {
      val batch = new BatchStatement()
      records.foreach {
        case AddRealTimeJob(calculationId, script, inputChannelIds, outputChannelId, startDate, endDate) =>
          val addJob = QueryBuilder.insertInto(TABLE_NAME)
            .value("calculation", calculationId)
            .value("script", script)
            .value("input_channels", inputChannelIds.asJava)
            .value("output_channel", outputChannelId)
            .value("start_date",startDate.toInstant(ZoneOffset.UTC).toEpochMilli)
            .value("end_date",endDate.toInstant(ZoneOffset.UTC).toEpochMilli)
          batch.add(addJob)

        case RemoveRealTimeJob(calculationId) =>
          val removeJob = QueryBuilder.delete().from(TABLE_NAME)
            .where(QueryBuilder.eq("calculation", calculationId))
          batch.add(removeJob)
      }
      Some(batch)
    }
  }


  def deserialize(job: String): Option[RealTimeJob] = {
    try {
      Some(JsonParser(job).convertTo[RealTimeJob])
    } catch {
      case _: ParsingException =>
        Log.error("Can't deserialize data record: " + job)
        None
    }
  }

}