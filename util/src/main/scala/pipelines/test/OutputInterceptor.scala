package pipelines.test

import java.io.{ ByteArrayOutputStream, PrintStream }

/**
 * Mixin trait to capture stdout and stderr, by intercepting Java's System.err and
 * System.out, then assert the contents are what's expected (or ignore them).
 * WARNING: Does not successfully capture output in all cases, including Java
 * logging libraries that are initialized first, even when configured to write to
 * the console, and possibly some multi-threaded apps. Hence, this class has
 * limited power.
 */
trait OutputInterceptor {

  // Set this to true in a test to add the debug print statements below:
  var dumpOutputStreams: Boolean = false

  protected lazy val name = getClass().getName()

  /**
   * Simply wrap a thunk to capture and ignore all stdout and stderr output.
   * @param test logic to execute.
   */
  def ignoreOutput(test: ⇒ Unit) = doCheckOutput(false)(test) { (outLines, errLines) => () }

  /**
   * Wrap a thunk to capture all stdout output, then assert it has exactly
   * the expected content. Output to stderr is ignored.
   * @param expectedOutLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param test logic to execute.
   */
  def expectOutput(expectedOutLines: Seq[String])(
    test: ⇒ Unit) = doCheckOutput(false)(test)(defaultCheck(true, expectedOutLines, OutputInterceptor.empty))

  /**
   * Wrap a thunk to capture all stdout and stderr output, then assert they have
   * exactly the expected content.
   * @param expectedOutLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param expectedErrLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param test logic to execute.
   */
  def expectOutput(
    expectedOutLines: Seq[String],
    expectedErrLines: Seq[String])(
    test: ⇒ Unit) = doCheckOutput(false)(test)(defaultCheck(false, expectedOutLines, expectedErrLines))

  /**
   * Wrap a thunk to capture all stdout output, then assert it has exactly the
   * expected content, after trimming leading and trailing whitespace on each line
   * and removing blank lines. (The expected strings are also trimmed this way).
   * @param expectedOutLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param test logic to execute.
   */
  def expectTrimmedOutput(
    expectedOutLines: Seq[String])(
    test: ⇒ Unit) = doCheckOutput(true)(test)(defaultCheck(true, expectedOutLines, OutputInterceptor.empty))

  /**
   * Wrap a thunk to capture all stdout and stderr output, then assert they have
   * exactly the expected content, after trimming leading and trailing whitespace
   * on each line and removing blank lines. (The expected strings are also trimmed
   * this way).
   * @param expectedOutLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param expectedErrLines sequence of lines expected, verbatim. Pass [[OutputInterceptor.empty]] if you expect NO output.
   * @param test logic to execute.
   */
  def expectTrimmedOutput(
    expectedOutLines: Seq[String],
    expectedErrLines: Seq[String])(
    test: ⇒ Unit) = doCheckOutput(true)(test)(defaultCheck(false, expectedOutLines, expectedErrLines))

  /**
   * Construct the default checker, which compares the actual with expected output,
   * verbatim. When trimming is request, it is done by [[doCheckOutput]] before
   * calling the function returned by this method.
   */
  protected def defaultCheck(
    ignoreErrOutput: Boolean,
    expectedOutLines: Seq[String],
    expectedErrLines: Seq[String]): (Seq[String], Seq[String]) => Unit =
    (outLines, errLines) => {
      assert(
        outLines.size == expectedOutLines.size, sizeDiffString("out", outLines, expectedOutLines))
      assert(
        outLines.toSeq == expectedOutLines, notEqualDiffString("out", outLines, expectedOutLines))
      if (ignoreErrOutput == false) {
        assert(
          errLines.size == expectedErrLines.size, sizeDiffString("err", errLines, expectedErrLines))
        assert(
          errLines.toSeq == expectedErrLines, notEqualDiffString("err", errLines, expectedErrLines))
      }
    }

  /**
   * Wrap a test to capture and all stdout and stderr output, then apply the
   * user-specified test function on the content. Use this function, for example,
   * when you want to check that some lines contain certain content, but ignore
   * everything else. The function will be passed a `Seq[String]` of the captured
   * stdout and a `Seq[String]` of the captured stderr. The function should perform
   * the required checks and assert/fail on error.
   * @param test logic to execute.
   * @param checkActualOutErrLines function to validate the output is correct
   */
  def checkOutput(test: ⇒ Unit)(checkActualOutErrLines: (Seq[String], Seq[String]) => Unit) =
    doCheckOutput(false)(test)(checkActualOutErrLines)

  /**
   * Wrap a test to capture and all stdout and stderr output, trim leading and
   * trailing whitespace on each line and remove blank lines, then apply the
   * user-specified test function on the content. Use this function, for example,
   * when you want to check that some lines contain certain content, but ignore
   * everything else. The function will be passed a `Seq[String]` of the captured
   * stdout and a `Seq[String]` of the captured stderr. The function should perform
   * the required checks and assert/fail on error.
   * @param test logic to execute.
   * @param checkActualOutErrLines function to validate the output is correct
   */
  def checkTrimmedOutput(test: ⇒ Unit)(checkActualOutErrLines: (Seq[String], Seq[String]) => Unit) =
    doCheckOutput(true)(test)(checkActualOutErrLines)

  protected def doCheckOutput(trim: Boolean)(test: ⇒ Unit)(checkActualOutErrLines: (Seq[String], Seq[String]) => Unit) {

    val outCapture = new ByteArrayOutputStream
    val errCapture = new ByteArrayOutputStream
    val outPrint = new PrintStream(outCapture)
    val errPrint = new PrintStream(errCapture)

    val saveOut = System.out // JDK
    val saveErr = System.err // JDK

    // Capture _both_ stdout and stderr hooks at the Java and Scala levels.
    try {
      Console.withOut(outCapture) {
        System.setOut(outPrint)
        Console.withErr(errCapture) {
          System.setErr(errPrint)
          test
        }
      }
    } finally {
      System.setOut(saveOut)
      System.setErr(saveErr)
    }

    val outLines1 = outCapture.toString.split("\n").toSeq
    val outLines = if (trim) outLines1.map(_.trim).filter(_.size >= 0) else outLines1
    val errLines1 = errCapture.toString.split("\n").toSeq
    val errLines = if (trim) errLines1.map(_.trim).filter(_.size >= 0) else errLines1
    val trimMsg = if (trim) " (trimmed)" else ""
    if (dumpOutputStreams) {
      println(outLines.mkString(s"$name - captured stdout$trimMsg:\n  ", "\n  ", "\n"))
      println(errLines.mkString(s"$name - captured stderr$trimMsg:\n  ", "\n  ", "\n"))
    }
    outPrint.close()
    errPrint.close()
    checkActualOutErrLines(outLines, errLines)
  }

  private def sizeDiffString(label: String, actual: Seq[String], expected: Seq[String]): String =
    diffString(s"$label - size mismatch: ${actual.size} != ${expected.size}", actual, expected)
  private def notEqualDiffString(label: String, actual: Seq[String], expected: Seq[String]): String =
    diffString(s"$label - mismatch", actual, expected)

  private def diffString(prefix: String, actual: Seq[String], expected: Seq[String]): String =
    s"""${name} - ${prefix} (Note: Seq() looks empty, but could be Seq("")):\nActual:   ${actual}\nExpected: ${expected}"""
}

object OutputInterceptor {
  /**
   * Pass this special value when you expect to output to the particular stream, e.g., no error output.
   */
  val empty = Array("")
}
