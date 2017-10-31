package carldata.dumper

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.RealTimeJobRecord
import com.datastax.driver.core.querybuilder.{Clause, QueryBuilder}
import com.datastax.driver.core.{BatchStatement, Statement}
import org.slf4j.LoggerFactory
import spray.json.JsonParser
import spray.json.JsonParser.ParsingException

object RealTimeProcessor {

  val TABLE_NAME = "real_time_jobs"

  private val Log = LoggerFactory.getLogger(this.getClass)

  def process(messages: Seq[String]): Option[Statement] = {

    QueryBuilder.delete().from(TABLE_NAME).where(QueryBuilder.eq("","")).and(QueryBuilder.eq("",""))
      .and(QueryBuilder.eq("",""))
    None
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