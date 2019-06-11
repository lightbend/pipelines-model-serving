package pipelines.examples.ingestor

import java.io.ByteArrayOutputStream

trait OutputInterceptor {

  def ignoreOutput[T](test: ⇒ T) {
    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream

    Console.withOut(outCapture) {
      Console.withErr(errCapture) {
        test
      }
    }
  }

  // Use `Seq("")` instead of Nil as the defaults, because that's what empty output will be.
  def expectOutput[T](
      expectedOutLines: Seq[String] = Seq(""),
      expectedErrLines: Seq[String] = Seq(""))(test: ⇒ T) {

    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream

    Console.withOut(outCapture) {
      Console.withErr(errCapture) {
        test
      }
    }

    val outLines = outCapture.toString.split("\n").toSeq
    assert(
      outLines.size == expectedOutLines.size,
      s"size mismatch: ${outLines.size} != ${expectedOutLines.size}. Actual = ${outLines}, expected = ${expectedOutLines}")
    assert(
      outLines == expectedOutLines,
      s"mismatch: ${outLines} != ${expectedOutLines}")

    val errLines = errCapture.toString.split("\n").toSeq
    assert(
      errLines.size == expectedErrLines.size,
      s"size mismatch: ${errLines.size} != ${expectedErrLines.size}")
    assert(
      errLines == expectedErrLines,
      s"mismatch: ${errLines} != ${expectedErrLines}. Actual = ${errLines}, expected = ${expectedErrLines}")
  }
}
