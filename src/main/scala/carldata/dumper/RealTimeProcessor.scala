package carldata.dumper

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, ErrorAction, RealTimeJobRecord, RemoveAction}
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
    val records = messages.flatMap(deserialize)
    if (records.isEmpty) None
    else {
      val batch = new BatchStatement()
      records.foreach(r => {
        if (r.action == AddAction) {
          println("Adding Job")
          val addJob = QueryBuilder.insertInto(TABLE_NAME)
            .value("calculation", r.calculationId)
            .value("script", r.script)
            .value("input_channels", r.inputChannelIds.asJava)
            .value("output_channel", r.outputChannelId)
          batch.add(addJob)
        } else if (r.action == RemoveAction) {
          println("Removing job")
          val removeJob = QueryBuilder.delete().from(TABLE_NAME)
            .where(QueryBuilder.eq("calculation", r.calculationId))
          batch.add(removeJob)
        } else if (r.action == ErrorAction) {
          Log.error("Error action occurred: " + r.toString)
        }
      })
      Some(batch)
    }
  }

  def deserialize(job: String): Option[RealTimeJobRecord] = {
    try {
      Some(JsonParser(job).convertTo[RealTimeJobRecord])
    } catch {
      case _: ParsingException =>
        Log.error("Can't deserialize data record: " + job)
        None
    }
  }

}