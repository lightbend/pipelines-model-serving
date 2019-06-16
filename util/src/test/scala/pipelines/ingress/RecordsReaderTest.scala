package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.util.test.OutputInterceptor

class RecordsReaderTest extends FunSpec with OutputInterceptor {

  val initializingMsgFmt = "RecordsReader: Initializing from resource %s"
  val badRecordMsgFmt = "Invalid record string: %s"
  val testGoodRecordsResources = Array("good-records1.csv", "good-records2.csv")
  val testBadRecordsResources = Array("bad-records.csv")
  def identityR = (r: String) => Right(r)
  def intStringTupleCSVParse = (r: String) => r.split(",") match {
    case Array(i, s) => try {
      Right(i.toInt -> s)
    } catch {
      case scala.util.control.NonFatal(e) => Left(e.toString)
    }
    case ary => Left(r)
  }

  describe("RecordsReader") {
    it("throws an exception if no resources are specified") {
      intercept[RecordsReader$NoResourcesSpecified$] {
        RecordsReader(Nil)(identityR)
      }
    }

    it("Loads one or more file resources from the classpath") {
      ignoreOutput {
        assert(RecordsReader(testGoodRecordsResources)(identityR).next() != null)
      }
    }

    it("Raises an exception if the resource doesn't exist") {
      ignoreOutput {
        intercept[RecordsReader.FailedToLoadResource] {
          RecordsReader(Seq("foobar"))(identityR)
        }
      }
    }

    describe("next") {
      it("Continuously rereads the resource until terminated") {
        val outMsgs1 = testGoodRecordsResources.map(s => initializingMsgFmt.format(s))
        val outMsgs = outMsgs1 ++ outMsgs1
        expectOutput(outMsgs) {
          val reader = RecordsReader(testGoodRecordsResources)(intStringTupleCSVParse)
          val actual = (0 until 12).foldLeft(Vector.empty[(Long, (Int, String))]) {
            (v, _) ⇒ v :+ reader.next()
          }
          val expected1 = Vector(
            (1, "one"),
            (2, "two"),
            (3, "three"),
            (4, "four"),
            (5, "five"),
            (6, "six"))
          val expected = (expected1 ++ expected1).zipWithIndex.map { case (tup, i) => ((i + 1).toLong, tup) }
          assert(actual == expected)
        }
      }

      it("Prints errors for bad records") {
        val outMsgs = testBadRecordsResources.map(s ⇒ initializingMsgFmt.format(s))
        // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
        val errMsgs = Array(
          badRecordMsgFmt.format("1,"),
          badRecordMsgFmt.format("two"),
          badRecordMsgFmt.format("3three"),
          badRecordMsgFmt.format("java.lang.NumberFormatException: For input string: \"four\""),
          badRecordMsgFmt.format("java.lang.NumberFormatException: For input string: \"\""))

        expectOutput(outMsgs, errMsgs) {
          intercept[RecordsReader.AllRecordsAreBad] {
            val reader = RecordsReader(testBadRecordsResources)(intStringTupleCSVParse)
            (0 until 5).foreach(_ ⇒ reader.next())
          }
        }
      }
    }
  }
}
