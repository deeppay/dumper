package carldata.dumper

import com.datastax.driver.core.Statement
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MainAppTest extends FlatSpec
  with Matchers
  with EmbeddedKafkaStreamsAllInOne {


  implicit val config = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
  def test_run(dbExecute: Statement => Boolean, data: Seq[String]): Unit = {
    val streamBuilder = new KStreamBuilder
    streamBuilder.stream(MainApp.DATA_TOPIC)

    runStreams(Seq(MainApp.DATA_TOPIC), streamBuilder) {
      val main = MainApp
      Future {
        data.foreach(d => publishStringMessageToKafka(MainApp.DATA_TOPIC, d))
        main.run("localhost:9092", "", dbExecute)
      }
      Future {
        consumeNumberStringMessagesFrom(MainApp.DATA_TOPIC, data.size)
        data.slice(0, data.size).foreach(d => publishStringMessageToKafka(MainApp.DATA_TOPIC, d))

      }
      Thread.sleep(6000)
    }

    Thread.sleep(2000)
    MainApp.stop()


  }

  "MainApp" should "read all data from kafka" in {
    val testData = Seq("{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.055\",\"value\":510.0}"
      , "{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.056\",\"value\":420.21}"
      , "{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.057\",\"value\":50.3}"
      , "{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.058\",\"value\":290.2}"
      , "{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.059\",\"value\":670.20}"
      , "{\"channelId\":\"theia-in-1\",\"timestamp\":\"2017-10-12T13:43:46.060\",\"value\":210.1}"
    )
    var checkList: Array[Boolean] = Array()
    def dbExecuter(): Statement => Boolean = {
      stmt => {
        try {
          checkList=  checkList :+ true
          true
        }
        catch {
          case e: Exception => {
            println("Exception occurred: " + e.toString())
            false
          }
        }
      }
    }

    test_run(dbExecuter(), testData)
    !checkList.contains(false)

    checkList.length shouldEqual testData.size
  }

}
