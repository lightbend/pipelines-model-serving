package pipelines.ingress

/**
 * Construct a RecordsFilesReader that provides an infinite stream of CVS records,
 * repeatedly reading them from the specified resources.
 * WARNING: This simple implementation does not handle nested, quoted, or escaped separators (e.g., ',')!
 */
object CSVReader {
  /**
   * Read from the file system.
   * @param resourcePaths file names within the file system.
   * @param separator to split the CSV string on.
   * @param dropFirstN mostly used to skip over column headers, if you know they are there. Otherwise, they will fail to parse.
   * @param parse function that coverts the `Array[String]` after splitting into records. If a line fails to parse, return a `Left[String]`.
   */
  def fromFileSystem[R](
    resourcePaths: Seq[String],
    separator: String = ",",
    dropFirstN: Int = 0)(
    parse: Array[String] ⇒ Either[String, R]): RecordsFilesReader[R] =
    RecordsFilesReader.fromFileSystem[R](
      resourcePaths: Seq[String], dropFirstN)(s ⇒ parse(s.split(separator)))

  /**
   * Read from the CLASSPATH.
   * @param resourcePaths file names within the CLASSPATH resources.
   * @param separator to split the CSV string on.
   * @param dropFirstN mostly used to skip over column headers, if you know they are there. Otherwise, they will fail to parse.
   * @param parse function that coverts the `Array[String]` after splitting into records. If a line fails to parse, return a `Left[String]`.
   */
  def fromClasspath[R](
    resourcePaths: Seq[String],
    separator: String = ",",
    dropFirstN: Int = 0)(
    parse: Array[String] ⇒ Either[String, R]): RecordsFilesReader[R] =
    RecordsFilesReader.fromClasspath[R](
      resourcePaths: Seq[String], dropFirstN)(s ⇒ parse(s.split(separator)))
}

