package pipelines.ingress

/**
 * Provides a potentially infinite stream of records, repeatedly reading them from
 * the specified resources.
 * @param resourcePaths the paths to resource files in the CLASSPATH
 * @param numberOfRecords how many to print before stopping. Default is never stop.
 */
final case class RecordsReader[R](
  resourcePaths: Seq[String])(
  parse: String => Either[String, R]) {

  if (resourcePaths.size == 0) throw RecordsReader.NoResourcesSpecified

  private def init(whichSource: Int): Iterator[String] = try {
    val name = resourcePaths(whichSource)
    // TODO: replace with proper info logging.
    println(s"RecordsReader: Initializing from resource $name")
    scala.io.Source.fromResource(name).getLines
  } catch {
    case scala.util.control.NonFatal(cause) ⇒
      throw RecordsReader.FailedToLoadResource(resourcePaths(whichSource), cause)
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

  final case object NoResourcesSpecified
    extends IllegalArgumentException(
      "No resources were specified from which to read records.")

  final case class FailedToLoadResource(resourcePath: String, cause: Throwable = null)
    extends IllegalArgumentException(
      s"Failed to load resource $resourcePath. Does it exist on the CLASSPATH?", cause)

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
      |  resource1 [...]    one or more paths to resources
      """.stripMargin)
      sys.exit(0)
    }
    def fa(args2: Seq[String], count_resources: (Int, Vector[String])): (Int, Vector[String]) = args2 match {
      case Nil => count_resources
      case ("-h" | "--help") +: tail => help()
      case ("-n" | "--n") +: nStr +: tail => fa(tail, (nStr.toInt, count_resources._2))
      case res +: tail => fa(tail, (count_resources._1, count_resources._2 :+ res))
    }
    val (count, resources) = fa(args, (100000, Vector.empty))

    // simply except the string...
    val reader = RecordsReader(resources)(s => Right(s))
    (1 to count).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
