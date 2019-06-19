package pipelines.examples.ingestor

import pipelines.examples.data._
import pipelines.ingress.RecordsReader

/**
 * To construct a WineRecords reader.
 */
object WineRecordsReader {

  /** Make a reader for [[WineRecord]]s. */
  def makeReader(resourcePaths: Seq[String]): RecordsReader[WineRecord] =
    RecordsReader.fromClasspath(resourcePaths)(csvParserWithSeparator(";"))

  /**
   * Used to construct the parser required by an instance of
   * [[pipelines.ingress.RecordsReader]] for [[WineRecord]] instances.
   */
  def csvParserWithSeparator(separator: String = ","): String ⇒ Either[String, WineRecord] =
    (line: String) ⇒ {
      val cols = line.split(separator)
      if (cols.length < 11) {
        Left(
          "Record does not have 11 fields after splitting string on ';': " + line)
      } else try {
        val dcols = cols.map(_.trim.toDouble)
        Right(WineRecord(
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
          Left(
            s"Failed to parse string $line. cause: $nf")
      }
    }

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    val count = if (args.length > 0) args(0).toInt else 100000

    val reader = WineRecordsReader.makeReader(WineDataIngressUtil.wineQualityRecordsResources)
    (1 to count).foreach { n ⇒
      val record = reader.next()
      println("%7d: %s".format(n, record))
    }
  }
}
