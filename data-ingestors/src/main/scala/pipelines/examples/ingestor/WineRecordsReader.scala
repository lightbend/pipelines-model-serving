package pipelines.examples.ingestor

import pipelines.examples.data._

/**
 * Provides an infinite stream of wine records, repeatedly reading them from
 * the specified resource.
 */
final case class WineRecordsReader(
    resourceNames: Seq[String],
    separator: String = ";") {

  assert(resourceNames.size > 0)

  private def init(whichSource: Int): Iterator[String] = try {
    val name = resourceNames(whichSource)
    println(s"WineRecordsReader: Initializing from resource $name")
    scala.io.Source.fromResource(name).getLines
  } catch {
    case cause: NullPointerException ⇒
      throw new IllegalArgumentException(s"Resource ${resourceNames(whichSource)} not found on the CLASSPATH", cause)
  }

  private var currentIndex = 0
  private var iterator: Iterator[String] = init(currentIndex)

  // TODO: IF there are NO valid records, effectively loops forever!
  def next(): WineRecord = {
    if (!iterator.hasNext) {
      currentIndex = (currentIndex + 1) % resourceNames.size
      iterator = init(currentIndex) // start over
    }
    parse(iterator.next()) match {
      case None         ⇒ next()
      case Some(record) ⇒ record
    }
  }

  def parse(line: String): Option[WineRecord] = {
    val cols = line.split(separator)
    if (cols.length < 11) {
      Console.err.println(
        "ERROR: record does not have 11 fields after splitting string on ';': " + line)
      None
    } else try {
      val dcols = cols.map(_.trim.toDouble)
      Some(WineRecord(
        fixed_acidity = dcols(0),
        volatile_acidity = dcols(1),
        citric_acid = dcols(2),
        residual_sugar = dcols(3),
        chlorides = dcols(4),
        free_sulfur_dioxide = dcols(5),
        total_sulfur_dioxide = dcols(6),
        density = dcols(7),
        pH = dcols(8),
        sulphates = dcols(9),
        alcohol = dcols(10),
        dataType = "wine"))
    } catch {
      case scala.util.control.NonFatal(nf) ⇒
        Console.err.println(
          s"ERROR: Failed to parse string $line. cause: $nf")
        None
    }
  }
}

object WineRecordsReader {
  def main(args: Array[String]): Unit = {
    val count = if (args.length > 0) args(0).toInt else 100000

    val reader = new WineRecordsReader(WineDataIngress.WineQualityRecordsResources)
    (1 to count).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
