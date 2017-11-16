package carldata.dumper

import java.time.{LocalDateTime, ZoneOffset}

import com.datastax.driver.core.BatchStatement
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._

class DeleteDataProcessorTest extends FlatSpec with Matchers {

  "Delete Data Processor" should "build 2 delete commands in date range" in {

    val dateFrom = LocalDateTime.of(2017, 10, 13, 0, 0, 0)
    val dateTo = LocalDateTime.of(2017, 10, 14, 0, 0, 0)

    val channelToRemove = "channel 1"
    val realTimeJobs = 1.to(2).map(i => RealTimeJob(Seq("fake", "channel " + i), "output " + i, dateFrom.toInstant(ZoneOffset.UTC).toEpochMilli,
      dateTo.toInstant(ZoneOffset.UTC).toEpochMilli))

    val deleteDataProcessor = new DeleteDataProcessor(null)
    val deleteStmt = deleteDataProcessor.buildDeleteStatements(channelToRemove, dateFrom.toInstant(ZoneOffset.UTC).toEpochMilli,
      dateTo.toInstant(ZoneOffset.UTC).toEpochMilli, realTimeJobs)

    val expected = List(
      "DELETE FROM data WHERE channel='output 1' AND timestamp>=1507852800000 AND timestamp<=1507939200000;",
      "DELETE FROM data WHERE channel='channel 1' AND timestamp>=1507852800000 AND timestamp<=1507939200000;"
    )

    val result = deleteStmt.asInstanceOf[BatchStatement].getStatements.asScala.map(_.toString).toList
    result shouldBe expected
  }
}
