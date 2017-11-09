package carldata.dumper

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


}


