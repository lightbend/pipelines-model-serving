package pipelines.ingress

import scala.io.{ BufferedSource, Source }
import java.io.{ FileInputStream, InputStream }
import java.util.zip.{ GZIPInputStream, ZipInputStream }
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream

/**
 * Provides an infinite stream of text-based records from one or more files in
 * a file system or on the CLASSPATH. It is assumed that the files contain one
 * record per line. This class loops through the list of resources, one at a time,
 * until all the records have been returned. Then it repeats this process "forever".
 * Note the records must be the same format (at least from the point of view of
 * the parse method you supply).
 * This class also handles the case where one or more of the files are actually
 * zipped (extension ".zip"), gzipped ("gz" or "gzip"), or bzipped ("bz2" or
 * "bzip2"). In fact, you can mix and match.
 * Mostly, this class is designed for testing purposes, as it's unlikely a
 * real-world application would read its input repeatedly and especially from
 * the CLASSPATH.
 * @param resourcePaths the paths to resource files in the CLASSPATH.
 * @param extraMissingResourceErrMsg an error message used when a resource doesn't exist.
 * @param getSource function that takes a resource name, opens it as appropriate, and returns a BufferedSource over the lines.
 * @param parse function that parses each line into a record, return an error as a `Left(String)`.
 */
final class RecordsFilesReader[R] protected[ingress] (
  val resourcePaths: Seq[String],
  val origin: String,
  dropFirstN: Int,
  getSource: String => BufferedSource,
  parse: String => Either[String, R]) {

  if (resourcePaths.size == 0) throw RecordsFilesReader.NoResourcesSpecified

  private var currentTotalCount: Long = 0L
  private var currentResourceIndex: Int = 0
  private var currentSource: BufferedSource = init(currentResourceIndex)
  private var iterator: Iterator[(String, Int)] = toIterator(currentSource)

  private def init(whichSource: Int): BufferedSource = {
    val currentResourceName = resourcePaths(whichSource)
    try {
      // TODO: replace with proper info logging.
      println(s"RecordsFilesReader: Initializing from resource $currentResourceName")
      getSource(currentResourceName)
    } catch {
      case scala.util.control.NonFatal(cause) ⇒
        throw RecordsFilesReader.FailedToLoadResource(currentResourceName, origin, cause)
    }
  }

  private def toIterator(source: BufferedSource): Iterator[(String, Int)] =
    source.getLines.drop(dropFirstN).zipWithIndex

  private def nextSource(): Unit = {
    currentResourceIndex = (currentResourceIndex + 1) % resourcePaths.size
    currentSource.close()
    currentSource = init(currentResourceIndex) // start over
    iterator = toIterator(currentSource)
  }

  private def failIfAllBad(): Unit =
    if (currentResourceIndex + 1 >= resourcePaths.size && currentTotalCount == 0)
      throw RecordsFilesReader.AllRecordsAreBad(resourcePaths)

  /**
   * Returns the next record, with the count of total records returned,
   * starting at 1. Transparently handles switching to a new resource in the
   * list of resources, when needed.
   */
  def next(): (Long, R) = {
    if (!iterator.hasNext) {
      failIfAllBad()
      nextSource()
    }
    val (line, lineNumber) = iterator.next()
    parse(line) match {
      case Left(error) ⇒
        // TODO: replace with proper info logging.
        Console.err.println(RecordsFilesReader.parseErrorMessageFormat.format(
          resourcePaths(currentResourceIndex), lineNumber, error, line))
        next()
      case Right(record) ⇒
        currentTotalCount += 1
        (currentTotalCount, record)
    }
  }
}

object RecordsFilesReader {

  val parseErrorMessageFormat = "ERROR (%s:%d) Invalid record string, %s. line = %s"

  /**
   * Load resources from a file system.
   * @param resourcePaths the paths (relative or absolute) in the file system to the resources.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromFileSystem[R](
    resourcePaths: Seq[String],
    dropFirstN: Int = 0)(
    parse: String => Either[String, R]) =
    new RecordsFilesReader[R](
      resourcePaths,
      "file system",
      dropFirstN,
      name => getSource(name, fromFile(name)),
      parse)

  /**
   * Load resources from the CLASSPATH.
   * @param resourcePaths the paths to the resources, relative to the root of the CLASSPATH.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromClasspath[R](
    resourcePaths: Seq[String],
    dropFirstN: Int = 0)(
    parse: String => Either[String, R]) =
    new RecordsFilesReader[R](
      resourcePaths,
      "CLASSPATH",
      dropFirstN,
      name => getSource(name, fromResource(name)),
      parse)

  def fromFile(name: String): InputStream = new FileInputStream(name)

  def fromResource(name: String): InputStream = {
    val classloader = Thread.currentThread().getContextClassLoader()
    classloader.getResourceAsStream(name) match {
      case null => throw FailedToLoadResource(name, "CLASSPATH")
      case is => is
    }
  }

  def getSource(name: String, is: InputStream): BufferedSource = {
    val extensionRE = raw"""^.*\.([^.]+)$$""".r
    val is2 = name match {
      case extensionRE("gz") | extensionRE("gzip") ⇒ new GZIPInputStream(is)
      case extensionRE("zip") ⇒ new ZipInputStream(is)
      case extensionRE("bz2") | extensionRE("bzip2") ⇒ new BZip2CompressorInputStream(is)
      case _ ⇒ is
    }
    scala.io.Source.fromInputStream(is2)
  }

  final case object NoResourcesSpecified
    extends IllegalArgumentException(
      "No resources were specified from which to read records.")

  val extraErrMsgs = Map(
    "file system" -> "Does the path exist?",
    "CLASSPATH" -> "Does it exist on the CLASSPATH?")
  final case class FailedToLoadResource(resourcePath: String, origin: String, cause: Throwable = null)
    extends IllegalArgumentException(
      s"Failed to load resource $resourcePath. ${extraErrMsgs(origin)}", cause)

  final case class AllRecordsAreBad(resourcePaths: Seq[String])
    extends IllegalArgumentException(
      s"All records found in the resources ${resourcePaths.mkString("[", ", ", "]")} failed to parse!!")

  /**
   * Program to test the logic.
   * To test the CSVReader wrapper logic, use pipelines.examples.airlineflights.main.Main
   * in project airlineFlightsModelServingPipeline. It also tests loading files from the
   * CLASSPATH.
   */
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
      |
      | To test the CSVReader wrapper logic, use pipelines.examples.airlineflights.main.Main
      | in project airlineFlightsModelServingPipeline. It also tests loading files from the
      | CLASSPATH.
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
      if (options.areFiles) RecordsFilesReader.fromFileSystem(options.resourcePaths)(s => Right(s))
      else RecordsFilesReader.fromClasspath(options.resourcePaths)(s => Right(s))
    (1 to options.maxRecords).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
