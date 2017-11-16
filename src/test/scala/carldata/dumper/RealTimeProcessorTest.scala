package carldata.dumper

import java.time.LocalDateTime

import carldata.hs.RealTime.{AddRealTimeJob, RealTimeJob, RemoveRealTimeJob}
import carldata.hs.RealTime.RealTimeJsonProtocol._
import com.datastax.driver.core.BatchStatement
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.collection.JavaConverters._

class RealTimeProcessorTest extends FlatSpec with Matchers {

  "RealTimeProcessor" should "build INSERT command on add action" in {

    val startDate = LocalDateTime.of(2017, 10, 13, 0, 0, 0)
    val endDate = LocalDateTime.of(2017, 10, 14, 0, 0, 0)

    val jobList: List[RealTimeJob] = List(
      AddRealTimeJob("calc 1", "test", Seq("in 1", "in 2"), "out 1",startDate,endDate),
      RemoveRealTimeJob("calc 2"),
      AddRealTimeJob("calc 3", "test", Seq("in 1", "in 2"), "out 3",startDate,endDate))

    val xs = jobList.map(_.toJson.prettyPrint)

    val expected = List(
      "INSERT INTO real_time_jobs (calculation,script,input_channels,output_channel,start_date,end_date) " +
        "VALUES ('calc 1','test',['in 1','in 2'],'out 1',1507852800000,1507939200000);",
      "DELETE FROM real_time_jobs WHERE calculation='calc 2';",
      "INSERT INTO real_time_jobs (calculation,script,input_channels,output_channel,start_date,end_date) " +
        "VALUES ('calc 3','test',['in 1','in 2'],'out 3',1507852800000,1507939200000);")

    val batch = RealTimeProcessor.process(xs).get.asInstanceOf[BatchStatement]
    val result = batch.getStatements.asScala.map(_.toString).toList
    result shouldBe expected
  }

  it should "not return command for empty sequence" in {
    RealTimeProcessor.process(List()) shouldBe None
  }

}
