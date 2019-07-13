package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.test.OutputInterceptor

class RecordsFilesReaderTest extends FunSpec with OutputInterceptor {

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

  describe("RecordsFilesReader") {
    describe("fromFileSystem()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsFilesReader$NoResourcesSpecified$] {
          RecordsFilesReader.fromFileSystem(Nil)(identityR)
        }
      }

      it("Loads one or more file resources from the file system") {
        ignoreOutput {
          assert(RecordsFilesReader.fromFileSystem(testGoodRecordsFiles)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsFilesReader.FailedToLoadResource] {
            RecordsFilesReader.fromFileSystem(Seq("foobar"))(identityR)
          }
        }
      }
    }

    describe("fromClasspath()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsFilesReader$NoResourcesSpecified$] {
          RecordsFilesReader.fromClasspath(Nil)(identityR)
        }
      }

      it("Loads one or more file resources from the classpath") {
        ignoreOutput {
          assert(RecordsFilesReader.fromClasspath(testGoodRecordsResources)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsFilesReader.FailedToLoadResource] {
            RecordsFilesReader.fromClasspath(Seq("foobar"))(identityR)
          }
        }
      }
    }

    val initializingMsgFmt = "RecordsFilesReader: Initializing from resource %s"

    def rereadTest(resourcePaths: Array[String], makeReader: => RecordsFilesReader[(Int, String)]): Unit = {
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

    def badRecordsTest(pathPrefix: String, resourcePaths: Array[String], makeReader: => RecordsFilesReader[(Int, String)]): Unit = {
      val outMsgs = resourcePaths.map(s ⇒ initializingMsgFmt.format(s))
      // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
      val fmt = RecordsFilesReader.parseErrorMessageFormat
      val file = s"${pathPrefix}bad-records.csv"
      val errMsgs = Array(
        fmt.format(file, 0, "1,", "1,"),
        fmt.format(file, 1, "two", "two"),
        fmt.format(file, 2, "3three", "3three"),
        fmt.format(file, 3, "java.lang.NumberFormatException: For input string: \"four\"", "four,4"),
        fmt.format(file, 4, "java.lang.NumberFormatException: For input string: \"\"", ",five"))

      expectOutput(outMsgs, errMsgs) {
        intercept[RecordsFilesReader.AllRecordsAreBad] {
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
            RecordsFilesReader.fromFileSystem[(Int, String)](testGoodRecordsFiles)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            "util/src/test/resources/",
            testBadRecordsFiles,
            RecordsFilesReader.fromFileSystem[(Int, String)](testBadRecordsFiles)(intStringTupleCSVParse))
        }
      }
    }

    describe("CLASSPATH reader") {
      describe("next") {
        it("Continuously rereads the resource until terminated") {
          rereadTest(
            testGoodRecordsResources,
            RecordsFilesReader.fromClasspath(testGoodRecordsResources)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            "",
            testBadRecordsResources,
            RecordsFilesReader.fromClasspath(testBadRecordsResources)(intStringTupleCSVParse))
        }
      }
    }
  }
}
