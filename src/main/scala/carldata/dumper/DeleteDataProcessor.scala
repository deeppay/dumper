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

}


