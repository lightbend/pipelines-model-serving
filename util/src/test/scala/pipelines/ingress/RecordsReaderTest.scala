package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.test.OutputInterceptor
import pipelines.logging.StdoutStderrLogger
import java.io.File
import java.net.URL

class RecordsReaderTest extends FunSpec with OutputInterceptor {

  RecordsReader.logger.setLogger(StdoutStderrLogger)

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
          // A different exception is thrown earlier if failIfMissing takes its default value of true
          RecordsReader.fromFileSystem(Nil, failIfMissing = false)(identityR)
        }
      }

      it("Loads one or more file resources from the file system") {
        ignoreOutput {
          assert(RecordsReader.fromFileSystem(testGoodRecordsFiles)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader.FailedToLoadResources[_]] {
            RecordsReader.fromFileSystem(Seq(new File("foobar")))(identityR)
          }
        }
      }
    }

    describe("fromClasspath()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          // A different exception is thrown earlier if failIfMissing takes its default value of true
          RecordsReader.fromClasspath(Nil, failIfMissing = false)(identityR)
        }
      }

      it("Loads one or more file resources from the classpath") {
        ignoreOutput {
          assert(RecordsReader.fromClasspath(testGoodRecordsResources)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader.FailedToLoadResources[_]] {
            RecordsReader.fromClasspath(Seq("foobar"))(identityR)
          }
        }
      }
    }

    describe("fromURLs()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          // A different exception is thrown earlier if failIfMissing takes its default value of true
          RecordsReader.fromURLs(Nil, failIfMissing = false)(identityR)
        }
      }

      it("Loads one or more file resources from the URL") {
        ignoreOutput {
          assert(RecordsReader.fromURLs(testGoodRecordsURLs)(identityR).next() != null)
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[RecordsReader$FailedToLoadResources] {
            RecordsReader.fromURLs(testBadRecordsURLs)(identityR)
          }
        }
      }
    }

    describe("fromConfiguration()") {
      it("throws an exception if an invalid configuration is specified") {
        intercept[RecordsReader$InvalidConfiguration] {
          RecordsReader.fromConfiguration("bad")(identityR)
        }
      }

      it("Loads one or more file resources based on the configuration") {
        ignoreOutput {
          assert(RecordsReader.fromConfiguration("records-reader-test")(identityR).next() != null)
        }
      }
    }

    val initializingMsgFmt1 = "INFO:  Reading resources from the %s: %s"
    val initializingMsgFmt2 = "INFO:  Initializing from resource %s (index: %d)"
    val kindMsgs = Map(
      RecordsReader.SourceKind.FileSystem -> "file system",
      RecordsReader.SourceKind.CLASSPATH  -> "CLASSPATH",
      RecordsReader.SourceKind.URLs       -> "URLs",
    )
    def rereadTest[T](
      kind: RecordsReader.SourceKind.Value,
      resourcePaths: Array[T],
      makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs = formatInitOutput(resourcePaths, kind, 2)
      expectTrimmedOutput(outMsgs.toSeq) {
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

    def badRecordsTest[T](
      kind: RecordsReader.SourceKind.Value,
      pathPrefix: String,
      resourcePaths: Array[T],
      makeReader: => RecordsReader[(Int, String)]): Unit = {
      val outMsgs = formatInitOutput(resourcePaths, kind, 1)
      // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
      val fmt = "WARN:  " + RecordsReader.parseErrorMessageFormat
      val file = s"${pathPrefix}bad-records.csv"
      val errMsgs = Array(
        fmt.format(file, 0, "1,", "1,"),
        fmt.format(file, 1, "two", "two"),
        fmt.format(file, 2, "3three", "3three"),
        fmt.format(file, 3, "java.lang.NumberFormatException: For input string: \"four\"", "four,4"),
        fmt.format(file, 4, "java.lang.NumberFormatException: For input string: \"\"", ",five"))

      expectTrimmedOutput(outMsgs, errMsgs) {
        intercept[RecordsReader.AllRecordsAreBad[_]] {
          val reader = makeReader
          (0 until 5).foreach(_ ⇒ reader.next())
        }
      }
    }

    def formatInitOutput[T](
      resourcePaths: Array[T],
      kind: RecordsReader.SourceKind.Value,
      repeat: Int = 1): Seq[String] = {

      val outMsgs =
        resourcePaths.zipWithIndex.map {
          case (rp, index) => initializingMsgFmt2.format(rp.toString, index)
        }
      val prefix =
        Seq(initializingMsgFmt1.format(
          kindMsgs(kind), resourcePaths.mkString("[", ", ", "]")))
      (1 to repeat).foldLeft(prefix)((s, _) => s ++ outMsgs)
    }

    describe("File system reader") {
      describe("next") {
        it("Continuously rereads the files until terminated") {
          rereadTest(
            RecordsReader.SourceKind.FileSystem,
            testGoodRecordsFiles,
            RecordsReader.fromFileSystem[(Int, String)](testGoodRecordsFiles)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            RecordsReader.SourceKind.FileSystem,
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
            RecordsReader.SourceKind.CLASSPATH,
            testGoodRecordsResources,
            RecordsReader.fromClasspath(testGoodRecordsResources)(intStringTupleCSVParse))
        }

        it("Prints errors for bad records") {
          badRecordsTest(
            RecordsReader.SourceKind.CLASSPATH,
            "",
            testBadRecordsResources,
            RecordsReader.fromClasspath(testBadRecordsResources)(intStringTupleCSVParse))
        }
      }
    }
  }
}
