package pipelinesx.reader

import org.scalatest.{ FunSpec, BeforeAndAfterEach, BeforeAndAfterAll }
import pipelinesx.test.OutputInterceptor
import java.io.File
import java.net.URL

class RecordsReaderTest extends FunSpec with BeforeAndAfterEach with BeforeAndAfterAll with OutputInterceptor {

  override def afterEach: Unit = {
    resetOutputs()
  }
  override def afterAll: Unit = {
    resetOutputs()
  }

  val clazz = this.getClass()
  val className = clazz.getName()

  val testGoodRecordsResources = Array("good-records1.csv", "good-records2.csv")
  val testBadRecordsResources = Array("bad-records.csv")
  val testGoodRecordsFiles = testGoodRecordsResources.map(s ⇒ new File("pipelinesx/src/test/resources/" + s))
  val testBadRecordsFiles = testBadRecordsResources.map(s ⇒ new File("pipelinesx/src/test/resources/" + s))
  val testGoodRecordsURLs = Array(new URL("https://lightbend.com/about-lightbend"))
  val testBadRecordsURLs = Array(new URL("http://example.foo"))
  def identityR = (r: String) ⇒ Right(r)

  def intStringTupleCSVParse(separator: String = ","): String ⇒ Either[String, (Int, String)] =
    (r: String) ⇒ r.split(separator) match {
      case Array(i, s) ⇒
        try { Right(i.toInt -> s) }
        catch { case scala.util.control.NonFatal(e) ⇒ Left(e.toString) }
      case _ ⇒ Left(r)
    }
  def nonEmptyIntArrayCSVParse(separator: String = ","): String ⇒ Either[String, Array[Int]] =
    (r: String) ⇒ {
      val array = r.split(separator)
      if (array.length == 0) Left("empty array!")
      else try {
        Right(array.map(_.toInt))
      } catch {
        case scala.util.control.NonFatal(e) ⇒ Left(s"Non-integer elements: $r. (${e})")
      }
    }

  describe("RecordsReader") {
    describe("fromFileSystem()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          // A different exception is thrown earlier if failIfMissing takes its default value of true
          ignoreOutput {
            RecordsReader.fromFileSystem(Nil, failIfMissing = false)(identityR)
            ()
          }
        }
      }

      it("Loads one or more file resources from the file system") {
        ignoreOutput {
          assert(RecordsReader.fromFileSystem(testGoodRecordsFiles)(identityR).next() != null)
          ()
        }
      }

      it("Raises an exception if the file doesn't exist") {
        intercept[RecordsReader.FailedToLoadResources[_]] {
          ignoreOutput {
            RecordsReader.fromFileSystem(Seq(new File("foobar")))(identityR)
            ()
          }
        }
      }
    }

    describe("fromClasspath()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          ignoreOutput {
            // A different exception is thrown earlier if failIfMissing takes its default value of true
            RecordsReader.fromClasspath(Nil, failIfMissing = false)(identityR)
            ()
          }
        }
      }

      it("Loads one or more file resources from the classpath") {
        ignoreOutput {
          assert(RecordsReader.fromClasspath(testGoodRecordsResources)(identityR).next() != null)
          ()
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        intercept[RecordsReader.FailedToLoadResources[_]] {
          ignoreOutput {
            RecordsReader.fromClasspath(Seq("foobar"))(identityR)
            ()
          }
        }
      }
    }

    describe("fromURLs()") {
      it("throws an exception if no resources are specified") {
        intercept[RecordsReader$NoResourcesSpecified$] {
          ignoreOutput {
            // A different exception is thrown earlier if failIfMissing takes its default value of true
            RecordsReader.fromURLs(Nil, failIfMissing = false)(identityR)
            ()
          }
        }
      }

      it("Loads one or more file resources from the URL") {
        ignoreOutput {
          assert(
            RecordsReader.fromURLs(testGoodRecordsURLs)(identityR).next() != null
          )
          ()
        }
      }

      it("Raises an exception if the resource doesn't exist") {
        intercept[RecordsReader$FailedToLoadResources] {
          ignoreOutput {
            RecordsReader.fromURLs(testBadRecordsURLs)(identityR)
            ()
          }
        }
      }
    }

    describe("fromConfiguration()") {
      it("throws an exception if the specified configuration root key is not found") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("bad")(identityR)
            ()
          }
        }
      }

      it("throws an exception if 'data-sources' under the specified configuration root key is not found") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-without-data-sources")(identityR)
            ()
          }
        }
      }

      it("throws an exception if 'which-source' is not found under 'data-sources'") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-without-which-source")(identityR)
            ()
          }
        }
      }

      it("throws an exception if 'which-source' is not 'CLASSPATH', 'FileSystem', or 'URLs'") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-empty-which-source")(identityR)
            ()
          }
        }
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-invalid-which-source")(identityR)
            ()
          }
        }
      }

      Seq("classpath", "filesystem", "urls").foreach { x ⇒
        it(s"throws an exception when $x source is specified, but the configuration for it is missing") {
          intercept[RecordsReader$InvalidConfiguration] {
            ignoreOutput {
              RecordsReader.fromConfiguration(s"records-reader-test-$x-without-$x")(identityR)
              ()
            }
          }
        }

        it(s"throws an exception when $x source is specified, but the configuration for it is empty") {
          intercept[RecordsReader$InvalidConfiguration] {
            ignoreOutput {
              RecordsReader.fromConfiguration(s"records-reader-test-$x-with-empty-$x")(identityR)
              ()
            }
          }
        }
      }

      it("throws an exception if the CLASSPATH 'paths' is empty") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-classpath-with-empty-paths")(identityR)
            ()
          }
        }
      }

      it("throws an exception if the CLASSPATH 'paths' entries are invalid") {
        intercept[RecordsReader$ConfigurationError] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-classpath-with-invalid-paths")(identityR)
            ()
          }
        }
      }

      it("throws an exception if the File System 'paths' and 'dir-paths' are both empty") {
        intercept[RecordsReader$InvalidConfiguration] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-filesystem-with-empty-paths-and-dir-paths")(identityR)
            ()
          }
        }
      }

      val colonDelimitedLine = Right(1 -> "7.4;0.7;0;1.9;0.076;11;34;0.9978;3.51;0.56;9.4;5")

      it("does not throw an exception if the File System 'dir-paths' entries is non-empty but 'file-name-regex' not found ('' is used)") {
        ignoreOutput {
          assert(colonDelimitedLine ===
            RecordsReader.fromConfiguration("records-reader-test-filesystem-with-nonempty-dir-paths-empty-file-name-regex")(identityR).next())
          ()
        }
      }

      it("finds the files matching the File System 'dir-paths' 'file-name-regex' entries") {
        ignoreOutput {
          assert(colonDelimitedLine ===
            RecordsReader.fromConfiguration("records-reader-test-filesystem-with-nonempty-dir-paths-nonempty-matching-file-name-regex")(identityR).next())
          ()
        }
      }

      it("throws an exception if the File System 'dir-paths' and 'file-name-regex' don't match any files") {
        intercept[RecordsReader$FailedToLoadResources] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-filesystem-with-nonempty-dir-paths-nonempty-nonmatching-file-name-regex")(identityR).next()
            ()
          }
        }
      }

      it("finds the File System 'paths' files instead of the 'dir-paths', if both are nonempty") {
        ignoreOutput {
          assert(Right(1 -> "1,") ===
            RecordsReader.fromConfiguration("records-reader-test-filesystem-with-nonempty-paths-and-dir-paths")(identityR).next())
          ()
        }
      }

      it("throws an exception if either of the URLs 'base-urls' or 'files' are empty") {
        intercept[RecordsReader$ConfigurationError] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-urls-with-empty-base-urls")(identityR).next()
            ()
          }
        }
        intercept[RecordsReader$ConfigurationError] {
          ignoreOutput {
            RecordsReader.fromConfiguration("records-reader-test-urls-with-empty-files")(identityR).next()
            ()
          }
        }
      }

      it("loads one or more file resources from the CLASSPATH when the configuration specifies that source") {
        ignoreOutput {
          assert(RecordsReader.fromConfiguration("records-reader-test-classpath")(identityR).next() != null)
          ()
        }
      }

      it("loads one or more file resources from the file system when the configuration specifies that source") {
        ignoreOutput {
          assert(RecordsReader.fromConfiguration("records-reader-test-filesystem")(identityR).next() != null)
          ()
        }
      }

      it("loads one or more file resources from URLs when the configuration specifies that source") {
        ignoreOutput {
          assert(RecordsReader.fromConfiguration("records-reader-test-urls")(identityR).next() != null)
          ()
        }
      }
    }

      // T is String, File, or URL
      def rereadTest[T](
          kind:          RecordsReader.SourceKind.Value,
          resourcePaths: Array[T],
          makeReader:    ⇒ RecordsReader[(Int, String)]): Unit = {
        val reader = makeReader
        val (errors, valids) = (0 until 12).foldLeft(
          (Vector.empty[(Long, String)], Vector.empty[(Long, (Int, String))])
        ) {
            case ((es, vs), _) ⇒ reader.next() match {
              case Left(x)  ⇒ (es :+ x, vs)
              case Right(x) ⇒ (es, vs :+ x)
            }
          }
        val expected1 = Vector(
          (1, "one"),
          (2, "two"),
          (3, "three"),
          (4, "four"),
          (5, "five"),
          (6, "six"))
        val expected = (expected1 ++ expected1).zipWithIndex.map { case (tup, i) ⇒ ((i + 1).toLong, tup) }
        assert(valids == expected)
        assert(errors.size == 0)
        ()
      }

      def badRecordsTest[T](
          kind:          RecordsReader.SourceKind.Value,
          resourcePaths: Array[T],
          makeReader:    ⇒ RecordsReader[(Int, String)]): Unit = {
        val expected = Vector(
          (1, "1,", "1,"),
          (2, "two", "two"),
          (3, "3three", "3three"),
          (4, "java.lang.NumberFormatException: For input string: \"four\"", "four,4"),
          (5, "java.lang.NumberFormatException: For input string: \"\"", ",five"))
          .map {
            case (i, error, line) ⇒
              val error2 = RecordsReader.parseErrorMessageFormat.format(resourcePaths(0), i - 1, error, line)
              (i, error2)
          }

        val reader = makeReader
        val (errors, valids) = (0 until 5).foldLeft(
          (Vector.empty[(Long, String)], Vector.empty[(Long, (Int, String))])
        ) {
            case ((es, vs), _) ⇒ reader.next() match {
              case Left(x)  ⇒ (es :+ x, vs)
              case Right(x) ⇒ (es, vs :+ x)
            }
          }
        assert(valids.size == 0)
        assert(errors == expected)
        ()
      }

    describe("File system reader") {
      describe("next") {
        it("Continuously rereads the files until terminated") {
          ignoreOutput {
            rereadTest(
              RecordsReader.SourceKind.FileSystem,
              testGoodRecordsFiles,
              RecordsReader.fromFileSystem[(Int, String)](testGoodRecordsFiles)(intStringTupleCSVParse()))
          }
        }

        it("Prints errors for bad records") {
          ignoreOutput {
            badRecordsTest(
              RecordsReader.SourceKind.FileSystem,
              testBadRecordsFiles,
              RecordsReader.fromFileSystem[(Int, String)](testBadRecordsFiles)(intStringTupleCSVParse()))
          }
        }
      }
    }

    describe("CLASSPATH reader") {
      describe("next") {
        it("Continuously rereads the resource until terminated") {
          ignoreOutput {
            rereadTest(
              RecordsReader.SourceKind.CLASSPATH,
              testGoodRecordsResources,
              RecordsReader.fromClasspath[(Int, String)](testGoodRecordsResources)(intStringTupleCSVParse()))
          }
        }

        it("Prints errors for bad records") {
          ignoreOutput {
            badRecordsTest(
              RecordsReader.SourceKind.CLASSPATH,
              testBadRecordsResources,
              RecordsReader.fromClasspath[(Int, String)](testBadRecordsResources)(intStringTupleCSVParse()))
          }
        }
      }
    }

    describe("Configuration reader") {
      describe("next") {
        it("Continuously rereads the resource until terminated") {
          ignoreOutput {
            rereadTest(
              RecordsReader.SourceKind.CLASSPATH,
              testGoodRecordsResources,
              RecordsReader.fromConfiguration[(Int, String)]("records-reader-test-classpath2")(intStringTupleCSVParse()))
          }
        }
      }
    }
  }
}
