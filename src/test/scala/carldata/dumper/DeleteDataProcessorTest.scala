package carldata.dumper

import java.time.LocalDateTime

import carldata.hs.DeleteData.DeleteDataJsonProtocol._
import carldata.hs.DeleteData.DeleteDataRecord
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

class DeleteDataProcessorTest extends FlatSpec with Matchers {

  "Delete Data Processor" should "build delete command in date range" in {

    val dateFrom = LocalDateTime.of(2015,1,1,0,0,0)
    val dateTo = LocalDateTime.of(2015,1,2,0,0,0)

    val records = 1.to(3).map(i => DeleteDataRecord("test","channel " + i,dateFrom,dateTo).toJson.prettyPrint).toList


  }

}
