package pipelines.examples.ingestor

import java.nio.file.FileSystems

import akka.NotUsed
import akka.stream.alpakka.file.scaladsl.FileTailSource

import akka.stream.scaladsl.Source
import akka.stream.ThrottleMode
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.Codecs._
import pipelines.examples.data.IndexedCSV

import scala.concurrent.duration._
import scala.util.control.NonFatal
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

class WineDataIngress extends SourceIngress[IndexedCSV] {

  override def createLogic = new SourceIngressLogic() {

    val whichSource = 0

    def source: Source[IndexedCSV, NotUsed] = {
      val util = OptionDataIngressUtil()
      val s1 = whichSource match {
        case 0 ⇒ util.readFromClasspath()
        case 1 ⇒ util.readFromFile()
        case 2 ⇒ util.cannedLines()
        case 3 ⇒ util.fakeLines()
        case _ ⇒ throw new RuntimeException(s"BUG: whichSource, $whichSource, not 0, 1, or 2")
      }
      s1.zipWithIndex
        .map(t ⇒ IndexedCSV(t._2, t._1))
    }
  }

}

object OptionDataIngressUtil {
  val defaultDataFile = "winequality_red.csv"
  // Experimentally chosen values to be "okay".
  val defaultDelayMillis = 1.seconds
  val defaultErrorRecordMultiple = 100
  val defaultCost = 1000
  val defaultMaxRandomValue = 7
  val defaultMaxBurst = 200
}

case class OptionDataIngressUtil(
    dataFile: String = OptionDataIngressUtil.defaultDataFile,
    delayMillis: FiniteDuration = OptionDataIngressUtil.defaultDelayMillis,
    errorRecordMultiple: Int = OptionDataIngressUtil.defaultErrorRecordMultiple,
    cost: Int = OptionDataIngressUtil.defaultCost,
    maxRandomValue: Int = OptionDataIngressUtil.defaultMaxRandomValue,
    maxBurst: Int = OptionDataIngressUtil.defaultMaxBurst) {

  /**
   * Read the CSV file from the classpath. This works after the project is built and in
   * deployed pipelines!
   * To run this in a unit test, etc., you'll need to build "package" first, so the data
   * file is staged in the correct target/scala-2.12/...jar file.
   * The Yak shaving here is pretty unbelievable...
   */
  def readFromClasspath(): Source[String, NotUsed] = try {
    val bs = scala.io.Source.fromResource(dataFile)
    if (bs.isEmpty) println(s"ERROR: OptionDataIngress: $dataFile found and is empty!")
    else println(s"INFO: OptionDataIngress: $dataFile found and has data!")

    val iterator: Iterator[String] = bs.getLines().map(_.toString)
    val iterable: scala.collection.immutable.Iterable[String] = iterator.toStream
    Source.apply[String](iterable)
      //      .drop(1) // drop the labels row
      .throttle(cost, delayMillis, maxBurst, costCalculation, ThrottleMode.Shaping) // "dribble" them out
    //     .throttle(1, delayMillis) // "dribble" them out
    //      .merge(badRecords())
  } catch {
    case NonFatal(th) ⇒
      println(s"ERROR: OptionDataIngress: $dataFile not found! $th")
      throw new RuntimeException(s"ERROR: OptionDataIngress: $dataFile not found!", th)
  }

  private def costCalculation(s: String): Int = scala.util.Random.nextInt(maxRandomValue) * s.length()

  /**
   * Read the CSV file from the file system. Not sure this works in deployment!
   */
  def readFromFile(): Source[String, NotUsed] = {
    FileTailSource.lines(
      path = FileSystems.getDefault.getPath("./datamodel/data/" + dataFile),
      maxLineSize = 8192,
      pollingInterval = 250.millis)
      .drop(1) // drop the labels row
      .throttle(cost, delayMillis, maxBurst, costCalculation, ThrottleMode.Shaping) // "dribble" them out
      //      .throttle(1, delayMillis.milliseconds) // "dribble" them out
      .merge(badRecords())
  }

  /**
   * Returns just `errorRecordMultiple` canned lines from the data file.
   * @return
   */
  def cannedLines(): Source[String, NotUsed] = {
    val recs: Array[String] = Array.fill[String](errorRecordMultiple)(
      "^VIX,2016-02-10 09:31:00,VIX,2016-02-10,10.000,c,0.00,0.00,0.00,0.00,0,0,0.00,0,0.00,0.00,0.00,0.00,0.00,0.0000,0.0000,0.000000,0.000000,0.000000,0.000000")
    Source((0 until errorRecordMultiple).map(i ⇒ recs(i)))
  }

  def fakeLines(): Source[String, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ createOptionRecord(1))
      .throttle(cost, delayMillis, maxBurst, costCalculation, ThrottleMode.Shaping) // "dribble" them out
      //      .throttle(1, delayMillis)
      .merge(badRecords())

  def createOptionRecord(stringRecord: Int): String = createOptionRecordArray(stringRecord).mkString(",")

  def createOptionRecordArray(stringRecord: Int): Array[String] =
    Array(
      "Test",
      dateNow,
      "Test",
      "0",
      "0",
      "Test" + stringRecord,
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "0",
      "-1",
      "0",
      "0",
      "0",
      "false"
    )

  def badRecords(): Source[String, NotUsed] =
    Source.repeat(NotUsed)
      .map(_ ⇒ createBadRecord(1))
      //      .throttle(cost, delayMillis, maxBurst, costCalculation, ThrottleMode.Shaping) // "dribble" them out
      .throttle(1, (errorRecordMultiple * delayMillis))

  /**
   * Create a fake record with two, invalid integer strings.
   */
  def createBadRecord(stringRecord: Int): String = {
    val ary = createOptionRecordArray(stringRecord)
    ary(4) = "bad"
    ary(6) = "bad"
    ary.mkString(",")
  }

  private def dateNow: String = {
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
    new DateTime().toString(fmt)
  }
}

