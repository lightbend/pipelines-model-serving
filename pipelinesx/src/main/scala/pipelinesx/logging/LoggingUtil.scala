package pipelinesx.logging

/** Miscellaneous utilities to assist in logging or similar formatted output. */
object LoggingUtil {

  /**
   * Helper that nicely formats an exception, its stack trace and all the causes
   * and their stack traces! calls {@link throwableToStrings} to construct an
   * array, which this method concatenates separated by the supplied delimiter.
   * @param th the Throwable
   * @param delimiter insert between each line.
   */
  def throwableToString(th: Throwable, delimiter: String = "\n"): String =
    throwableToStrings(th).mkString(delimiter)

  /**
   * Helper that nicely formats and logs an exception, including its stack trace
   * and all the causes and their stack traces!
   */
  def throwableToStrings(th: Throwable): Vector[String] = {
    var vect = Vector.empty[String]
    var throwable = th
    vect = vect :+ (s"$throwable: " + formatStackTrace(throwable))
    throwable = throwable.getCause()
    while (throwable != null) {
      vect = vect :+ "\nCaused by: "
      vect = vect :+ (s"$throwable: " + formatStackTrace(throwable))
      throwable = throwable.getCause()
    }
    vect
  }

  private def formatStackTrace(th: Throwable): String =
    th.getStackTrace().mkString("\n  ", "\n  ", "\n")

}
