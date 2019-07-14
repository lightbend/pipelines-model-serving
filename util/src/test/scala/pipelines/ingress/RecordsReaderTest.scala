package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.test.OutputInterceptor
import java.io.File
import java.net.URL

class RecordsReaderTest extends FunSpec with OutputInterceptor {

  val testGoodRecordsResources = Array("good-records1.csv", "good-records2.csv")
  val testBadRecordsResources = Array("bad-records.csv")
  val testGoodRecordsFiles = testGoodRecordsResources.map(s => new File("util/src/test/resources/" + s))
  val testBadRecordsFiles = testBadRecordsResources.map(s => new File("util/src/test/resources/" + s))
  val testGoodRecordsURLs = Array(new URL("http://stat-computing.org/dataexpo/2009/1987.csv.bz2"))
  val testBadRecordsURLs = Array(new URL("http://example.foo"))
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
          intercept[RecordsReader.FailedToLoadResources] {
            RecordsReader.fromFileSystem(Seq(new File("foobar")))(identityR)
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
          intercept[RecordsReader.FailedToLoadResources] {
            RecordsReader.fromClasspath(Seq("foobar"))(identityR)
          }
        }
      }
    }

    describe("fromURLs()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          RecordsReader.fromURLs(Nil)(identityR)
        }
      }

      it("Loads one or more file resources from the URL") {
        ignoreOutput {
          assert(RecordsReader.fromURLs(testGoodRecordsURLs)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader.FailedToLoadResources] {
            RecordsReader.fromURLs(testBadRecordsURLs)(identityR)
          }
        }
      }
    }

    describe("fromConfiguration()") {
      it("throws an exception if an invalid configuration is specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          RecordsReader.fromConfiguration("bad")(identityR)
        }
      }

      it("Loads one or more file resources based on the configuration") {
        ignoreOutput {
          assert(RecordsReader.fromConfiguration("records-reader-test")(identityR).next() != null)
        }
      }
    }

    val initializingMsgFmt = "RecordsReader: Initializing from resource %s"

    def rereadTest[T](resourcePaths: Array[T], makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs1 = resourcePaths.map(rp => initializingMsgFmt.format(rp.toString))
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

    def badRecordsTest[T](pathPrefix: String, resourcePaths: Array[T], makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs = resourcePaths.map(rp ⇒ initializingMsgFmt.format(rp.toString))
      // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
      val fmt = RecordsReader.parseErrorMessageFormat
      val file = s"${pathPrefix}bad-records.csv"
      val errMsgs = Array(
        fmt.format(file, 0, "1,", "1,"),
        fmt.format(file, 1, "two", "two"),
        fmt.format(file, 2, "3three", "3three"),
        fmt.format(file, 3, "java.lang.NumberFormatException: For input string: \"four\"", "four,4"),
        fmt.format(file, 4, "java.lang.NumberFormatException: For input string: \"\"", ",five"))

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
            "util/src/test/resources/",
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
            "",
            testBadRecordsResources,
            RecordsReader.fromClasspath(testBadRecordsResources)(intStringTupleCSVParse))
        }
      }
    }
  }
}
