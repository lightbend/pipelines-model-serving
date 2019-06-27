package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.util.test.OutputInterceptor

class RecordsReaderTest extends FunSpec with OutputInterceptor {

  val initializingMsgFmt = "RecordsReader: Initializing from resource %s"
  val badRecordMsgFmt = "Invalid record string: %s"
  val testGoodRecordsResources = Array("good-records1.csv", "good-records2.csv")
  val testBadRecordsResources = Array("bad-records.csv")
  val testGoodRecordsFiles = testGoodRecordsResources.map(s => "util/src/test/resources/" + s)
  val testBadRecordsFiles = testBadRecordsResources.map(s => "util/src/test/resources/" + s)
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
    describe("fromFileSystem()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          RecordsReader.fromFileSystem(Nil)(identityR)
        }
      }

      it("Loads one or more file resources from the file system") {
        ignoreOutput {
          assert(RecordsReader.fromFileSystem(testGoodRecordsFiles)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader.FailedToLoadResource] {
            RecordsReader.fromFileSystem(Seq("foobar"))(identityR)
          }
        }
      }
    }

    describe("fromClasspath()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          RecordsReader.fromClasspath(Nil)(identityR)
        }
      }

      it("Loads one or more file resources from the classpath") {
        ignoreOutput {
          assert(RecordsReader.fromClasspath(testGoodRecordsResources)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader.FailedToLoadResource] {
            RecordsReader.fromClasspath(Seq("foobar"))(identityR)
          }
        }
      }
    }

    def rereadTest(resourcePaths: Array[String], makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs1 = resourcePaths.map(s => initializingMsgFmt.format(s))
      val outMsgs = outMsgs1 ++ outMsgs1
      expectOutput(outMsgs) {
        val reader = makeReader
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

    def badRecordsTest(resourcePaths: Array[String], makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs = resourcePaths.map(s ⇒ initializingMsgFmt.format(s))
      // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
      val errMsgs = Array(
        badRecordMsgFmt.format("1,"),
        badRecordMsgFmt.format("two"),
        badRecordMsgFmt.format("3three"),
        badRecordMsgFmt.format("java.lang.NumberFormatException: For input string: \"four\""),
        badRecordMsgFmt.format("java.lang.NumberFormatException: For input string: \"\""))

      expectOutput(outMsgs, errMsgs) {
        intercept[RecordsReader.AllRecordsAreBad] {
          val reader = makeReader
          (0 until 5).foreach(_ ⇒ reader.next())
        }
      }
    }

    describe("File system reader") {
      describe("next") {
        it("Continuously rereads the files until terminated") {
          rereadTest(
            testGoodRecordsFiles,
            RecordsReader.fromFileSystem[(Int, String)](testGoodRecordsFiles)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            testBadRecordsFiles,
            RecordsReader.fromFileSystem[(Int, String)](testBadRecordsFiles)(intStringTupleCSVParse))
        }
      }
    }

    describe("CLASSPATH reader") {
      describe("next") {
        it("Continuously rereads the resource until terminated") {
          rereadTest(
            testGoodRecordsResources,
            RecordsReader.fromClasspath(testGoodRecordsResources)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            testBadRecordsResources,
            RecordsReader.fromClasspath(testBadRecordsResources)(intStringTupleCSVParse))
        }
      }
    }
  }
}
