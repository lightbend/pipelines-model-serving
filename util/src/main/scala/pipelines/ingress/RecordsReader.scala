package pipelines.ingress

/**
 * Provides a potentially infinite stream of records, repeatedly reading them from
 * the specified resources. Note, use the RecordsReader companion object helper
 * methods to construct a reader.
 * @param resourcePaths the paths to resource files in the CLASSPATH
 * @param numberOfRecords how many to print before stopping. Default is never stop.
 */
final case class RecordsReader[R](
  resourcePaths: Seq[String],
  extraMissingResourceErrMsg: String,
  getLines: String => Iterator[String],
  parse: String => Either[String, R]) {

  if (resourcePaths.size == 0) throw RecordsReader.NoResourcesSpecified

  private def init(whichSource: Int): Iterator[String] = {
    val name = resourcePaths(whichSource)
    try {
      // TODO: replace with proper info logging.
      println(s"RecordsReader: Initializing from resource $name")
      getLines(name)
    } catch {
      case scala.util.control.NonFatal(cause) ⇒
        throw RecordsReader.FailedToLoadResource(name, extraMissingResourceErrMsg, cause)
    }
  }

  private var currentIndex: Long = 0L
  private var currentSourceIndex: Int = 0
  private var iterator: Iterator[String] = init(currentSourceIndex)

  private def failIfAllBad(): Unit =
    if (currentSourceIndex + 1 >= resourcePaths.size && currentIndex == 0)
      throw RecordsReader.AllRecordsAreBad(resourcePaths)

  /**
   * Returns the next record, with the count of total records returned,
   * starting at 1. Transparently handles switching to a new resource in the
   * list of resources, when needed.
   */
  def next(): (Long, R) = {
    if (!iterator.hasNext) {
      failIfAllBad()
      currentSourceIndex = (currentSourceIndex + 1) % resourcePaths.size
      iterator = init(currentSourceIndex) // start over
    }
    parse(iterator.next()) match {
      case Left(error) ⇒
        // TODO: replace with proper info logging.
        Console.err.println("Invalid record string: " + error)
        next()
      case Right(record) ⇒
        currentIndex += 1
        (currentIndex, record)
    }
  }
}

object RecordsReader {

  def fromFileSystem[R](
    resourcePaths: Seq[String])(
    parse: String => Either[String, R]) = new RecordsReader[R](
    resourcePaths,
    extraErrMsgFile,
    name => scala.io.Source.fromFile(name).getLines,
    parse)

  def fromClasspath[R](
    resourcePaths: Seq[String])(
    parse: String => Either[String, R]) = new RecordsReader[R](
    resourcePaths,
    extraErrMsgClasspath,
    name => scala.io.Source.fromResource(name).getLines,
    parse)

  final case object NoResourcesSpecified
    extends IllegalArgumentException(
      "No resources were specified from which to read records.")

  val extraErrMsgFile = "Does the path exist?"
  val extraErrMsgClasspath = "Does it exist on the CLASSPATH?"
  final case class FailedToLoadResource(resourcePath: String, extraMsg: String, cause: Throwable = null)
    extends IllegalArgumentException(
      s"Failed to load resource $resourcePath. $extraMsg", cause)

  final case class AllRecordsAreBad(resourcePaths: Seq[String])
    extends IllegalArgumentException(
      s"All records found in the resources ${resourcePaths.mkString("[", ", ", "]")} failed to parse!!")

  def main(args: Array[String]): Unit = {
    def help(): Nothing = {
      println("""
      |scala pipelines.ingress.RecordsRecorder [-h|--help] [-n | --n N] resource1 [... resources]
      |where:
      |  -h | --help        print this message and exit
      |  -n | --n  N        print N records and exit
      |  -f | --files       treat the specified resources as file paths (default)
      |  -c | --classpath   expect the specified resources to be found on the CLASSPATH
      |  resource1 [...]    one or more paths to resources
      """.stripMargin)
      sys.exit(0)
    }

    final case class Options(
      resourcePaths: Vector[String] = Vector.empty,
      areFiles: Boolean = true,
      maxRecords: Int = 100000)

    def fa(args2: Seq[String], options: Options): Options = args2 match {
      case Nil => options
      case ("-h" | "--help") +: tail => help()
      case ("-n" | "--n") +: nStr +: tail => fa(tail, options.copy(maxRecords = nStr.toInt))
      case ("-f" | "--files") +: tail => fa(tail, options.copy(areFiles = true))
      case ("-c" | "--classpath") +: tail => fa(tail, options.copy(areFiles = false))
      case res +: tail => fa(tail, options.copy(resourcePaths = options.resourcePaths :+ res))
    }
    val options = fa(args, Options())

    // simply except the string...
    val reader =
      if (options.areFiles) RecordsReader.fromFileSystem(options.resourcePaths)(s => Right(s))
      else RecordsReader.fromClasspath(options.resourcePaths)(s => Right(s))
    (1 to options.maxRecords).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
