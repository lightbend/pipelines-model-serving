package pipelines.ingress

import scala.io.{ BufferedSource, Source }
import java.io.{ File, FilenameFilter, FileInputStream, FileOutputStream, InputStream }
import java.util.zip.{ GZIPInputStream, ZipInputStream }
import java.net.URL
import scala.collection.JavaConverters._
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import pipelines.config.ConfigUtil
import pipelines.config.ConfigUtil.implicits._
import pipelines.logging.{ MutableLogger, LoggingUtil }

/**
 * Provides an infinite stream of text-based records from one or more files in
 * a file system, on the CLASSPATH or downloaded from URLs.
 * It is assumed that the files contain one record per line. This class loops
 * through the list of resources, one at a time, until all the records have been
 * returned. Then it repeats this process "forever". (Downloads from URLs are
 * _not_ repeated)
 * Note the records must be the same format (at least from the point of view of
 * the parse method you supply).
 * This class also handles the case where one or more of the files are actually
 * zipped (extension ".zip"), gzipped ("gz" or "gzip"), or bzipped ("bz2" or
 * "bzip2"). In fact, you can mix and match.
 * Mostly, this class is designed for testing purposes, as it's unlikely a
 * real-world application would read its input repeatedly and especially from
 * the CLASSPATH.
 * The expected configuration in application.conf should follow this structure,
 * although it is only required if you use the factory method {@link RecordsReader.fromConfiguration}:
 * ```
 * data-sources : {
 *   which-source: "URLs",
 *   from-classpath : {
 *     paths: [ ]             // load this list of paths.
 *   },
 *   from-file-system : {
 *     dir-paths: [ ],        // Load all from a directory, ...
 *     file-name-regex: "",   // ... whose names match this regular expression. ("" for no filtering)
 *                            // OR,
 *     paths: [ ]             // load this list of paths.
 *   },
 *   from-urls : {
 *     base-urls: [           // Load all from these URL, ...
 *       "http://stat-computing.org/dataexpo/2009/"
 *     ],
 *     files: [               // ... combined with these files, but if empty, just use the base-urls
 *       "1987.csv.bz2",
 *       "1990.csv.bz2",
 *       "1995.csv.bz2",
 *       "2000.csv.bz2",
 *       "2005.csv.bz2"
 *     ]
 *   }
 * }
 * ```
 * Note that `which-source` determines which if any of the three sources are used.
 * The others are ignored, but switching to another kind of source is simply a
 * matter of changing this property. Note the comments that some approaches are
 * not yet implemented.
 * The type parameter `R` is the record type.
 * The type parameter `S` is the "source" type, e.g., File, URL, String, etc.
 * @param resourcePaths the paths to resource files in the CLASSPATH.
 * @param extraMissingResourceErrMsg an error message used when a resource doesn't exist.
 * @param getSource function that takes a resource name, opens it as appropriate, and returns a BufferedSource over the lines.
 * @param parse function that parses each line into a record, return an error as a `Left(String)`.
 */
trait RecordsReader[R] {
  def next(): (Long, R)
}

final class RecordsReaderImpl[R, S] protected[ingress] (
  val resourcePaths: Seq[S],
  val origin: RecordsReader.SourceKind.Value,
  dropFirstN: Int,
  getSource: S => BufferedSource,
  parse: String => Either[String, R]) extends RecordsReader[R] {

  if (resourcePaths.size == 0) throw RecordsReader.NoResourcesSpecified

  private var currentTotalCount: Long = 0L
  private var currentResourceIndex: Int = 0
  private var currentSource: BufferedSource = init(currentResourceIndex)
  private var iterator: Iterator[(String, Int)] = toIterator(currentSource)

  private def init(whichSource: Int): BufferedSource = {
    val currentResource = resourcePaths(whichSource)
    try {
      RecordsReader.logger.info(s"Initializing from resource $currentResource")
      getSource(currentResource)
    } catch {
      case scala.util.control.NonFatal(cause) ⇒
        throw RecordsReader.FailedToLoadResources(Seq(currentResource.toString), origin, cause)
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
      throw RecordsReader.AllRecordsAreBad(resourcePaths.map(_.toString))

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
        RecordsReader.logger.warn(RecordsReader.parseErrorMessageFormat.format(
          resourcePaths(currentResourceIndex), lineNumber, error, line))
        next()
      case Right(record) ⇒
        currentTotalCount += 1
        (currentTotalCount, record)
    }
  }
}

object RecordsReader {

  object SourceKind extends Enumeration {
    type SourceKind = Value
    val CLASSPATH, FileSystem, URLs = Value
  }
  import SourceKind._

  val parseErrorMessageFormat = "(%s:%d) Invalid record string, %s. line = %s"

  val logger: MutableLogger = LoggingUtil.getLogger(RecordsReader.getClass)

  /**
   * Load resources from a file system.
   * @param resourcePaths the file paths in the file system to the resources.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param failIfMissing fail if any of the specified files can't be found.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromFileSystem[R](
    resourcePaths: Seq[File],
    dropFirstN: Int = 0,
    failIfMissing: Boolean = false)(
    parse: String => Either[String, R]): RecordsReader[R] = {
    checkFiles(resourcePaths) match {
      case Nil => // okay
      case seq =>
        if (failIfMissing) throw FailedToLoadResources(seq.map(_.toString), FileSystem)
        else logger.warn(s"Some files are missing: ${resourcePaths.mkString(", ")}")
    }

    new RecordsReaderImpl[R, File](
      resourcePaths,
      FileSystem,
      dropFirstN,
      path => getSource(path.getCanonicalPath, fromFile(path)),
      parse)
  }

  /**
   * Load resources from the CLASSPATH.
   * @param resourcePaths the paths to the resources, relative to the root of the CLASSPATH.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param failIfMissing fail if any of the specified files can't be found.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromClasspath[R](
    resourcePaths: Seq[String],
    dropFirstN: Int = 0,
    failIfMissing: Boolean = false)(
    parse: String => Either[String, R]): RecordsReader[R] = {
    checkResources(resourcePaths) match {
      case Nil => // okay
      case seq =>
        if (failIfMissing) throw FailedToLoadResources(seq, CLASSPATH)
        else logger.warn(
          s"Some files are missing on the CLASSPATH: ${resourcePaths.mkString(", ")}")
    }

    new RecordsReaderImpl[R, String](
      resourcePaths,
      CLASSPATH,
      dropFirstN,
      name => getSource(name, fromResource(name)),
      parse)
  }

  /**
   * Download files from URLs to a local "temp file" location, then read the contents
   * from there.
   * Messages are logged indicating the temporary file name for the corresponding URL.
   * @param resourceURLs the URLs to use to retrieve the resources.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param failIfMissing fail if any of the specified files can't be downloaded.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromURLs[R](
    resourceURLs: Seq[URL],
    dropFirstN: Int = 0,
    failIfMissing: Boolean = false)(
    parse: String => Either[String, R]): RecordsReader[R] = {
    val (errors, paths) = download(resourceURLs)
    if (errors.size > 0) {
      if (failIfMissing) throw FailedToLoadResources(errors, URLs)
      else logger.warn(s"Some downloads failed: ${errors.mkString(",")}")
    }

    // We now instantiate a File-based reader:
    new RecordsReaderImpl[R, File](
      paths,
      URLs,
      dropFirstN,
      path => getSource(path.getCanonicalPath(), fromFile(path)),
      parse)
  }

  /**
   * Use the configuration to determine how record files are provided, then construct
   * a reader for that source. (Only one kind of source is supported.) Independent of
   * failIfMissing, an exception is thrown if we can't determine a source from the
   * configuration, which probably means it is mis-configured. See the requirements in
   * the class's documentation.
   * @param configurationKeyRoot the root configuration where the specification is found.
   * @param dropFirstN drop the first records, primarily to support CSV headers.
   * @param failIfMissing fail if any of the specified files can't be loaded.
   * @param parse function that parses each line into a record, return an error as a `Left(String)`.
   */
  def fromConfiguration[R](
    configurationKeyRoot: String,
    dropFirstN: Int = 0,
    failIfMissing: Boolean = false)(
    parse: String => Either[String, R]): RecordsReader[R] =
    determineSource(configurationKeyRoot) match {
      case FileSystem =>
        val paths = determineFilesFromConfiguration(configurationKeyRoot)
        fromFileSystem(paths, dropFirstN, failIfMissing)(parse)
      case CLASSPATH =>
        val paths = determineClasspathResourcesFromConfiguration(configurationKeyRoot)
        fromClasspath(paths, dropFirstN, failIfMissing)(parse)
      case URLs =>
        val urls = determineURLsFromConfiguration(configurationKeyRoot)
        fromURLs(urls, dropFirstN, failIfMissing)(parse)
    }

  protected def whichSource(configKeyRoot: String) =
    configKeyRoot + ".data-sources.which-source"

  /**
   * Looking at the loaded configuration, determine how resources should be loaded.
   * Currently only allows one source, the file system, the CLASSPATH, or URLs.
   */
  def determineSource(configKeyRoot: String): SourceKind = {
    val source = whichSource(configKeyRoot)
    ConfigUtil.default.get[String](source) match {
      case Some(flag) =>
        val f = flag.toLowerCase
        if (f.startsWith("file")) SourceKind.FileSystem
        else if (f.startsWith("url")) SourceKind.URLs
        else if (f.startsWith("class")) SourceKind.CLASSPATH
        else throw InvalidConfiguration(Seq(source), s"The value $flag is not a valid kind of record source.")
      case _ => throw InvalidConfiguration(Seq(source))
    }
  }

  protected def fromFile(path: File): InputStream = new FileInputStream(path)
  protected def fromFile(path: String): InputStream = fromFile(new File(path))

  protected def fromResource(path: String): InputStream = {
    val classloader = Thread.currentThread().getContextClassLoader()
    classloader.getResourceAsStream(path) match {
      case null => throw FailedToLoadResources(Seq(path), CLASSPATH)
      case is => is
    }
  }

  /**
   * Verify that the files exist in the file system.
   * @return Nil if all exist, otherwise return the ones not found.
   */
  protected def checkFiles(paths: Seq[File]): Seq[File] = {
    paths.foldLeft(Vector.empty[File]) {
      case (vect, path) =>
        if (path.exists()) vect else vect :+ path
    }
  }

  /**
   * Verify that the resources exist on the classpath.
   * @return Nil if all exist, otherwise return the ones not found.
   */
  protected def checkResources(paths: Seq[String]): Seq[String] = {
    val classloader = Thread.currentThread().getContextClassLoader()
    paths.foldLeft(Vector.empty[String]) {
      case (vect, path) =>
        classloader.getResource(path) match {
          case null => vect :+ path
          case _ => vect
        }
    }
  }

  protected def download(urls: Seq[URL]): (Vector[String], Vector[File]) =
    urls.foldLeft(Vector.empty[String] -> Vector.empty[File]) {
      case ((errors, files), url) =>
        val fileNameParts = url.toString.split("/").last.split("\\.")
        val (prefix1, suffix1) = fileNameParts.splitAt(fileNameParts.length - 1)
        val prefix = if (prefix1.size > 0) prefix1 else Array("file") // hmm
        val suffix = if (suffix1.size > 0) suffix1 else Array("data") // hmm
        download(url, prefix.mkString("."), suffix.head) match {
          case Left(error) => (errors :+ error, files)
          case Right(file) => (errors, files :+ file)
        }
    }

  // Copied some of this logic from ByteArrayReader. TODO: Merge??
  protected def download(
    url: URL, prefix: String, suffix: String): Either[String, File] = try {
    val suffix2 = if (suffix.length > 0) "." + suffix else suffix
    val target = File.createTempFile(prefix, suffix2)
    logger.info(s"Downloading $url to local file $target")

    val len = 1024 * 1024 // arbitrary size
    val buffer = Array.fill[Byte](len)(0)
    val is = url.openStream()
    val os = new FileOutputStream(target)
    var count = is.read(buffer)
    while (count > 0) {
      os.write(buffer, 0, count)
      count = is.read(buffer)
    }
    os.flush()
    os.close()
    Right(target)
  } catch {
    case scala.util.control.NonFatal(th) => Left(s"Failed to download URL $url and write to local file: $th")
  }

  protected def determineFilesFromConfiguration(configKeyRoot: String): Seq[File] = {
    // Works correctly even if the string is empty.
    def makeFilenameFilter(regexString: String) = new FilenameFilter {
      import scala.util.matching.Regex
      val re = new Regex(Regex.quote(regexString))

      def accept(dir: File, name: String): Boolean = re.findAllIn(name).size > 0
    }

    def addFiles(root: String, regexString: String, filter: FilenameFilter): Vector[File] = {
      val dir = new File(root)
      if (dir.exists) {
        val files = dir.listFiles(filter).toVector
        if (files.size == 0) logger.warn(s"No file found in $root for regex $regexString!")
        files
      } else {
        logger.warn(s"Directory $root doesn't exist or couldn't be read!")
        Vector.empty
      }
    }

    val fsp = configKeyRoot + ".data-sources.from-file-system.paths"
    val fsfp = configKeyRoot + ".data-sources.from-file-system.folder-paths"
    val fnre = configKeyRoot + ".data-sources.from-file-system.file-name-regex"
    ConfigUtil.default.get[Seq[String]](fsp) match {
      case Some(list) if list.size > 0 => list.map(p => new File(p))
      case _ =>
        val regexString = ConfigUtil.default.getOrElse[String](fnre)("")
        ConfigUtil.default.get[Seq[String]](fsfp) match {
          case Some(dirs) if dirs.size > 0 =>
            val filter = makeFilenameFilter(regexString)
            dirs.foldLeft(Vector.empty[File]) {
              case (v, d) => v ++ addFiles(d, regexString, filter)
            }
          case _ => throw InvalidConfiguration(Seq(fsfp))
        }
    }
  }

  protected def determineClasspathResourcesFromConfiguration(configKeyRoot: String): Seq[String] = {
    val cpp = configKeyRoot + ".data-sources.from-classpath.paths"
    ConfigUtil.default.get[Seq[String]](cpp) match {
      case Some(list) if list.size > 0 => list
      case _ => throw InvalidConfiguration(Seq(cpp))
    }
  }

  protected def determineURLsFromConfiguration(configKeyRoot: String): Seq[URL] = {
    def combine(urls: Seq[String], files: Seq[String]): Seq[URL] =
      urls.foldLeft(Vector.empty[URL]) {
        case (v, url) =>
          val url2 = if (url.endsWith("/")) url else url + "/"
          val fullURLs = files.map(f => new URL(url2 + f))
          v ++ fullURLs
      }

    val bu = configKeyRoot + ".data-sources.from-urls.base-urls"
    val f = configKeyRoot + ".data-sources.from-urls.files"
    val bu2 = ConfigUtil.default.get[Seq[String]](bu)
    val f2 = ConfigUtil.default.get[Seq[String]](f)
    (bu2, f2) match {
      case (Some(urls), Some(files)) if urls.size > 0 =>
        if (files.size > 0) combine(urls, files) else urls.map(url => new URL(url))
      case _ => throw InvalidConfiguration(Seq(bu, f))
    }
  }

  protected def getSource(name: String, is: InputStream): BufferedSource = {
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

  /** If the keys were found with unexpected values, put the values in the message string. */
  final case class InvalidConfiguration(keys: Seq[String], message: String = "")
    extends IllegalArgumentException(
      s"The configuration loaded from application.conf, etc. was missing one or more expected keys or unexpected values were returned: ${keys.mkString(", ")}. $message")

  protected val extraErrMsgs = Map(
    FileSystem -> "Do the paths exist?",
    CLASSPATH -> "Do they exist on the CLASSPATH?",
    URLs -> "Do the URLs exist?")
  final case class FailedToLoadResources(resourcePaths: Seq[String], origin: SourceKind.Value, cause: Throwable = null)
    extends IllegalArgumentException(
      s"Failed to load resources ${resourcePaths.mkString(", ")}. ${extraErrMsgs(origin)}", cause)

  final case class AllRecordsAreBad(resourcePaths: Seq[String])
    extends IllegalArgumentException(
      s"All records found in the resources ${resourcePaths.mkString("[", ", ", "]")} failed to parse!!")
}

/**
 * Program to test the logic.
 */
object RecordsReaderMain {

  def main(args: Array[String]): Unit = {
    def help(): Nothing = {
      println("""
      |scala pipelines.ingress.RecordsRecorder [-h|--help] [-n | --n N] resource1 [... resources]
      |where:
      |  -h | --help        print this message and exit
      |  -n | --n  N        print N records and exit
      |  -f | --files       treat the specified resources as file system paths (default)
      |  -c | --classpath   treat the specified resources as file paths  on the CLASSPATH
      |  -u | --urls        treat the specified resources as URLs
      |  resource1 [...]    one or more paths to resources
      """.stripMargin)
      sys.exit(0)
    }

    import RecordsReader.SourceKind
    import RecordsReader.SourceKind._

    final case class Options(
      resourcePaths: Vector[String] = Vector.empty,
      kind: SourceKind.Value = FileSystem,
      maxRecords: Int = 100000)

    def fa(args2: Seq[String], options: Options): Options = args2 match {
      case Nil => options
      case ("-h" | "--help") +: tail => help()
      case ("-n" | "--n") +: nStr +: tail => fa(tail, options.copy(maxRecords = nStr.toInt))
      case ("-f" | "--files") +: tail => fa(tail, options.copy(kind = FileSystem))
      case ("-c" | "--classpath") +: tail => fa(tail, options.copy(kind = CLASSPATH))
      case ("-u" | "--urls") +: tail => fa(tail, options.copy(kind = URLs))
      case res +: tail => fa(tail, options.copy(resourcePaths = options.resourcePaths :+ res))
    }
    val options = fa(args, Options())

    val reader = options.kind match {
      case FileSystem =>
        val paths = options.resourcePaths.map(p => new File(p))
        RecordsReader.fromFileSystem(paths)(s => Right(s))
      case CLASSPATH =>
        RecordsReader.fromClasspath(options.resourcePaths)(s => Right(s))
      case URLs =>
        val urls = options.resourcePaths.map(p => new URL(p))
        RecordsReader.fromURLs(urls)(s => Right(s))
    }

    (1 to options.maxRecords).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
