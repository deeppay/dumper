package carldata.dumper

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import com.datastax.driver.core.BatchStatement
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.collection.JavaConverters._

/** Embedded kafka runs on port 6001 */
class DataProcessorTest extends FlatSpec with Matchers {

  "Data Processor" should "build INSERT command" in {
    val now = LocalDateTime.of(2015,1,1,0,0,0)
    val records = 1.to(3).map(i => DataRecord("t1", now.plusSeconds(i), i.toFloat).toJson.prettyPrint).toList
    val expected = List(
      "INSERT INTO data (channel,timestamp,value) VALUES ('t1',1420070401000,1.0);",
      "INSERT INTO data (channel,timestamp,value) VALUES ('t1',1420070402000,2.0);",
      "INSERT INTO data (channel,timestamp,value) VALUES ('t1',1420070403000,3.0);"
    )

    val batch = DataProcessor.process(records).get.asInstanceOf[BatchStatement]
    val result = batch.getStatements.asScala.map(_.toString).toList
    result shouldBe expected
  }

  it should "not return command for empty sequence" in {
    DataProcessor.process(List()) shouldBe None
  }
}
