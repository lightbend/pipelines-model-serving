package pipelines.examples.util.test

import java.io.ByteArrayOutputStream

/**
 * Mixin trait to capture stdout and stderr, then assert the content is what's expected (or ignore it).
 */
trait OutputInterceptor {

  // Set this to true in a test to add the debug print statements below:
  var dumpOutputStreams: Boolean = false

  protected lazy val name = getClass().getSimpleName()

  /**
   * Simply wrap a thunk to capture and ignore all stdout and stderr output.
   */
  def ignoreOutput[T](test: ⇒ T) = checkOutput(test) { (outLines, errLines) => () }

  /**
   * Wrap a thunk to capture and all stdout and stderr output, then assert they have exactly
   * the expected content.
   * Uses `Seq("")` instead of Nil as the defaults, because that's what empty output will be.
   */
  def expectOutput[T](
    expectedOutLines: Seq[String] = Seq(""),
    expectedErrLines: Seq[String] = Seq(""))(test: ⇒ T) = checkOutput(test) { (outLines, errLines) =>

    assert(
      outLines.size == expectedOutLines.size, sizeDiffString(outLines, expectedOutLines))
    assert(
      outLines == expectedOutLines, notEqualDiffString(outLines, expectedOutLines))
    assert(
      errLines.size == expectedErrLines.size, sizeDiffString(errLines, expectedErrLines))
    assert(
      errLines == expectedErrLines, notEqualDiffString(errLines, expectedErrLines))
  }

  /**
   * Wrap a thunk to capture and all stdout and stderr output, then apply the user-specified tests on the
   * content. Use this function, for example, when you want to check that some lines contain certain content,
   * but ignore everything else. The function will be passed a `Seq[String]` of the captured stdout and a
   * `Seq[String]` of the captured stderr. It should perform the required checks and assert/fail on error.
   */
  def checkOutput[T](test: ⇒ T)(checkActualOutErrLines: (Seq[String], Seq[String]) => Unit) {

    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream

    Console.withOut(outCapture) {
      Console.withErr(errCapture) {
        test
      }
    }

    val outLines = outCapture.toString.split("\n").toSeq
    val errLines = errCapture.toString.split("\n").toSeq
    if (dumpOutputStreams) {
      println(outLines.mkString(s"$name - captured stdout:\n  ", "\n  ", "\n"))
      println(errLines.mkString(s"$name - captured stderr:\n  ", "\n  ", "\n"))
    }
    checkActualOutErrLines(outLines, errLines)
  }

  private def sizeDiffString(actual: Seq[String], expected: Seq[String]): String =
    diffString(s"size mismatch: ${actual.size} != ${expected.size}", actual, expected)
  private def notEqualDiffString(actual: Seq[String], expected: Seq[String]): String =
    diffString("mismatch", actual, expected)

  private def diffString(prefix: String, actual: Seq[String], expected: Seq[String]): String =
    s"${name} - ${prefix}:\nActual:   ${actual}\nExpected: ${expected}"

}
