package pipelines.ingress

import org.scalatest.FunSpec
import pipelines.test.OutputInterceptor

class ByteArrayReaderTest extends FunSpec with OutputInterceptor {

  describe("ByteArrayReader") {
    describe("fromFileSystem()") {
      it("returns a Left(error) if the resource isn't found") {
        ByteArrayReader.fromFileSystem("foo") match {
          case Left(error @ _) => // pass
          case Right(bytes) => fail("Returned bytes for a non-existent file!")
        }
      }

      it("returns a byte array for the contents of a file from the file system") {
        ByteArrayReader.fromFileSystem("build.sbt") match {
          case Left(error) => fail(s"Failed to return bytes for the file build.sbt: error = $error")
          case Right(bytes @ _) => // pass
        }
      }

      it("the returned byte array is the contents of the file") {
        ByteArrayReader.fromFileSystem("build.sbt") match {
          case Left(error) => fail(s"Failed to return bytes for the file build.sbt: error = $error")
          case Right(bytes) =>
            val expected = scala.io.Source.fromFile("build.sbt").getLines.reduceLeft(_ + "\n" + _)
            (bytes zip expected.getBytes).zipWithIndex foreach {
              case ((a, b), i) =>
                assert(a == b, s"$i, $a != $b")
            }
        }
      }
    }

    describe("fromClasspath()") {
    }
  }
}
