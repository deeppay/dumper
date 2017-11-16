package carldata.dumper

import java.time.{LocalDateTime, ZoneOffset}

import carldata.dumper.DeleteDataProcessor.RealTimeInfo
import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class DeleteDataProcessorTest extends FlatSpec with Matchers {

  "Delete Data Processor" should "build 2 delete commands in date range" in {

    val dateFrom = LocalDateTime.of(2017, 10, 13, 0, 0, 0)
    val dateTo = LocalDateTime.of(2017, 10, 14, 0, 0, 0)

    val records = Seq[String](DeleteDataRecord("actionId","channel 1",dateFrom,dateTo).toJson.prettyPrint)

    val realTimeJobs = 1.to(2).map(i =>  RealTimeInfo(Seq("fake", "channel " + i), "output " + i, dateFrom.toInstant(ZoneOffset.UTC).toEpochMilli,
      dateTo.toInstant(ZoneOffset.UTC).toEpochMilli)).toList

    val deleteDataProcessor = new DeleteDataProcessor(null)
    val deleteStmt = deleteDataProcessor.processMessages(records,realTimeJobs)

    val expected = List(
      "DELETE FROM data WHERE channel='channel 1' AND timestamp>=1507852800000 AND timestamp<=1507939200000;",
      "DELETE FROM data WHERE channel='output 1' AND timestamp>=1507852800000 AND timestamp<=1507939200000;"
    )

    val result = deleteStmt.map(_.toString).toList
    result shouldBe expected
  }
}
