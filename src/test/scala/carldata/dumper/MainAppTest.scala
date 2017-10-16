package carldata.dumper

import java.time.LocalDateTime

import carldata.hs.Data.DataJsonProtocol._
import carldata.hs.Data.DataRecord
import com.datastax.driver.core.Statement
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreams
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class MainAppTest extends FlatSpec
  with Matchers
  with EmbeddedKafkaStreams {

  implicit val config: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  def testRun(data: Seq[String]): Seq[Statement] = {

    var checkList: ListBuffer[Statement] = ListBuffer()
    var maxSeconds = 20

    def dbExecuter(stmt: Statement): Boolean = {
      checkList = checkList += stmt
      true
    }

    val streamBuilder = new KStreamBuilder
    streamBuilder.stream(MainApp.DATA_TOPIC)
    EmbeddedKafka.start
    val main = MainApp
    Future {
      main.run("localhost:9092", "", dbExecuter)
    }

    while (maxSeconds > 0 && checkList.isEmpty) {
      publishStringMessageToKafka(MainApp.DATA_TOPIC, "{\"channelId\":\"t\",\"timestamp\":\"2017-10-12T13:43:46.060\",\"value\":0}")
      Thread.sleep(1000)
      maxSeconds = maxSeconds - 1
    }
    checkList.clear()

    data.foreach(d => publishStringMessageToKafka(MainApp.DATA_TOPIC, d))

    while (maxSeconds > 0 && checkList.size < data.size) {
      Thread.sleep(1000)
      maxSeconds = maxSeconds - 1
    }

    main.stop()
    EmbeddedKafka.stop
    checkList
  }

  "MainApp" should "read all data from kafka" in {
    val testData = Seq(DataRecord("test-in-1", LocalDateTime.now, 1.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 11.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 120.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 130.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 140.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 150.0f)
      , DataRecord("test-in-1", LocalDateTime.now.plusSeconds(1), 160.0f)
    )

    val xs = testRun(testData.map(x => DataJsonFormat.write(x).toString()))


    xs.length shouldEqual testData.size
  }

}
