package carldata.dumper

import carldata.hs.RealTime.RealTimeJsonProtocol._
import carldata.hs.RealTime.{AddAction, RealTimeJobRecord, RemoveAction}
import com.datastax.driver.core.BatchStatement
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.collection.JavaConverters._

class RealTimeProcessorTest extends FlatSpec with Matchers {

  "RealTimeProcessor" should "build INSERT command on add action" in {

    val records = 1.to(3).map(i => RealTimeJobRecord(AddAction, "calc " + i, "test", Seq("in 1", "in 2"), "out " + i)
      .toJson.prettyPrint).toList

    val expected = List(
      "INSERT INTO real_time_jobs (calculation,script,input_channels,output_channel) VALUES ('calc 1','test',['in 1','in 2'],'out 1');",
      "INSERT INTO real_time_jobs (calculation,script,input_channels,output_channel) VALUES ('calc 2','test',['in 1','in 2'],'out 2');",
      "INSERT INTO real_time_jobs (calculation,script,input_channels,output_channel) VALUES ('calc 3','test',['in 1','in 2'],'out 3');")

    val batch = RealTimeProcessor.process(records).get.asInstanceOf[BatchStatement]
    val result = batch.getStatements.asScala.map(_.toString).toList
    result shouldBe expected
  }

  "RealTimeProcessor" should "build DELETE command on remove action" in {
    val records = 1.to(3).map(i => RealTimeJobRecord(RemoveAction, "calc " + i, "test", Seq("in 1", "in 2"), "out " + i)
      .toJson.prettyPrint).toList

    val expected = List(
      "DELETE FROM real_time_jobs WHERE calculation='calc 1';",
      "DELETE FROM real_time_jobs WHERE calculation='calc 2';",
      "DELETE FROM real_time_jobs WHERE calculation='calc 3';")

    val batch = RealTimeProcessor.process(records).get.asInstanceOf[BatchStatement]
    val result =  batch.getStatements.asScala.map(_.toString).toList
    result shouldBe expected
  }

  it should "not return command for empty sequence" in {
    RealTimeProcessor.process(List()) shouldBe None
  }

}
