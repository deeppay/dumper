package carldata.dumper

import java.time.LocalDateTime

import org.scalatest.{FlatSpec, Matchers}
import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import com.datastax.driver.core.BatchStatement
import spray.json._


/** Embedded kafka runs on port 6001 */
class DataProcessorTest extends FlatSpec with Matchers {

  "Data Processor" should "build INSERT command" in {
    val now = LocalDateTime.now()
    val records = 1.to(3).map(i => DataRecord("t1", now.plusSeconds(1), i.toFloat).toJson.prettyPrint).toList
    val expected =
      """
        |
      """.stripMargin

    val batch = DataProcessor.process(records).get.asInstanceOf[BatchStatement]
    batch.size() shouldBe 3
  }

  it should "not return command for empty sequence" in {
    DataProcessor.process(List()) shouldBe None
  }
}
