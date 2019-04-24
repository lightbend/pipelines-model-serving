package pipelines.examples.ingestor

import akka.NotUsed
import akka.stream.ThrottleMode
import akka.stream.scaladsl.Source
import pipelines.akkastream.scaladsl._
import pipelines.examples.data.Codecs._
import pipelines.examples.data._

import scala.concurrent.duration._
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Random

class WineDataIngress extends SourceIngress[IndexedCSV] {

  override def createLogic = new SourceIngressLogic() {

    val whichSource = 0

    def source: Source[IndexedCSV, NotUsed] = {
      val s1 = Source.repeat(NotUsed)
        .map(_ ⇒ getRandomWineRecord())
        .throttle(50, 1.seconds, 200, ThrottleMode.Shaping) // "dribble" them out
        //      .throttle(1, delayMillis)
        .merge(badRecords())

      s1.zipWithIndex
        .map(t ⇒ IndexedCSV(t._2, t._1))
    }

    def getRandomWineRecord(): String = {
      val fixedAcidity = generateRandomNumber(4.6, 15.9).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val volitaleAcidity = generateRandomNumber(.12, 1.58).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
      val citricAcid = generateRandomNumber(0, 1).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      val residualSugar = generateRandomNumber(.9, 3.5).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val chlorides = generateRandomNumber(.012, .611).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
      val freeSulfurDioxide = generateRandomNumber(1, 72).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble
      val totalSulfurDioxide = generateRandomNumber(6, 289).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
      val density = generateRandomNumber(.99, 1.004).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      val ph = generateRandomNumber(2.74, 4.010).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      val sulphates = generateRandomNumber(.33, 2).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
      val alcohol = generateRandomNumber(8.4, 14.9).setScale(0, BigDecimal.RoundingMode.HALF_UP).toDouble

      fixedAcidity + ";" + volitaleAcidity + ";" + citricAcid + ";" + residualSugar + ";" + chlorides + ";" + freeSulfurDioxide + ";" + totalSulfurDioxide + ";" + density + ";" + ph + ";" + sulphates + ";" + alcohol
    }

    private def generateRandomNumber(start: Double, end: Double): BigDecimal = {
      val random = new Random().nextDouble

      val result = start + (random * (end - start))
      result
    }

    def badRecords(): Source[String, NotUsed] =
      Source.repeat(NotUsed)
        .map(_ ⇒ createBadRecord(1))
        //      .throttle(cost, delayMillis, maxBurst, costCalculation, ThrottleMode.Shaping) // "dribble" them out
        .throttle(1, (100 * 1.seconds))

    def createBadRecord(stringRecord: Int): String = {
      val ary = createOptionRecordArray(stringRecord)
      ary(4) = "bad"
      ary(6) = "bad"
      ary.mkString(",")
    }

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

    private def dateNow: String = {
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
      new DateTime().toString(fmt)
    }

  }

}
