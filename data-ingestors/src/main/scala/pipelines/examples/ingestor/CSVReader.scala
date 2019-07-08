package pipelines.examples.ingestor

import java.util.zip.GZIPInputStream
import scala.io.Source

/**
 * Provides an infinite stream of CVS records, repeatedly reading them from
 * the specified resources.
 * This class handles the case where one or more of the resource files are
 * actually gzipped, i.e., they have the extension, "*.gz" or "*.gzip".
 * WARNING: This simple implementation does not handle nested, quoted, or escaped delimeters!
 * @param resourceNames file names within the class path resources.
 * @param separator to split the CSV string on.
 * @param dropFirstLines mostly used to skip over column headers, if you know they are there. Otherwise, they will fail to parse.
 * @param parse parses the `Array[String]` after splitting into records. If a line fails to parse, it prints a warning, then moves to the next line. Hence, only valid records are returned.
 */
final case class CSVReader[R](
    resourceNames: Seq[String],
    separator: String = ",",
    dropFirstLines: Int = 0)(
    parse: CSVReader.Parser[R]) {

  assert(resourceNames.size > 0)

  private var currentResourceName = ""
  private var currentResourceIndex = 0
  private var iterator: Iterator[String] = init(currentResourceIndex)
  private var lineNumber = 0

  private def init(whichSource: Int): Iterator[String] = try {
    currentResourceName = resourceNames(whichSource)
    println(s"CSVReader: Initializing from resource $currentResourceName")
    getLines(currentResourceName).drop(dropFirstLines)
  } catch {
    case cause: NullPointerException ⇒
      throw new IllegalArgumentException(s"Resource $currentResourceName not found on the CLASSPATH", cause)
  }

  // TODO: IF there are NO valid records, effectively loops forever!
  def next(): R = {
    if (!iterator.hasNext) {
      currentResourceIndex = (currentResourceIndex + 1) % resourceNames.size
      iterator = init(currentResourceIndex) // start over
      lineNumber = 0
    }
    val line = iterator.next()
    lineNumber += 1
    val tokens = line.split(separator)
    parse(tokens) match {
      case Left(error) ⇒
        Console.err.println(s"""ERROR: ($currentResourceName:$lineNumber) Failed to parse line "$line" with separator "$separator": error = $error""")
        next()
      case Right(record) ⇒ record
    }
  }

  def getLines(name: String): Iterator[String] = {
    val classloader = Thread.currentThread().getContextClassLoader()
    val ris = classloader.getResourceAsStream(name)
    val is = if (name.endsWith(".gz") || name.endsWith(".gzip")) {
      new GZIPInputStream(ris)
    } else { ris }
    Source.fromInputStream(is).getLines
  }
}

object CSVReader {
  type Parser[R] = Array[String] ⇒ Either[String, R]
}

