package pipelines.examples.ingestor

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source

import scala.concurrent.duration._

object DataIngressTestRunner {

  def help() = {
    println(
      s"""
        |OptionDataIngressTestRunner \\
        |  [-h|--help]                 \\   # Show help and exit
        |  [-f|--data-file] f          \\   # Which data file. default: ${OptionDataIngressUtil.defaultDataFile}
        |  [-d|--delay-millis] d       \\   # Approx delay between records. default: ${OptionDataIngressUtil.defaultDelayMillis}
        |  [-e|--error-record-mult] e  \\   # Approx number of real records to fake error records. default: ${OptionDataIngressUtil.defaultErrorRecordMultiple}
        |  [-c|--cost] c               \\   # "cost" for throttling calculation records. default: ${OptionDataIngressUtil.defaultCost}
        |  [--max-random] mr           \\   # For computing random values for delays, the max integer. default: ${OptionDataIngressUtil.defaultMaxRandomValue}
        |  [--max-burst] mb            \\   # Max records to "burst" at a time. default: ${OptionDataIngressUtil.defaultMaxBurst}
        |  [file|class|fake|canned]    \\   # Data source. Defaults to "class"
        |""".stripMargin)
  }

  /**
   * Tests the reading of the data file.
   * With no options or "file", attempts to read from the file in src/main/resources.
   * With "classpath", attempts to read from the classpath (assumes src/main/resources).
   * To run the "classpath" option, you'll need to build "package" first, so the data
   * file is staged in the correct target/scala-2.12/...jar file.
   */
  def main(args: Array[String]): Unit = {

    case class Options(
        whichOne: String = "class",
        dataFile: String = OptionDataIngressUtil.defaultDataFile,
        delayMillis: FiniteDuration = OptionDataIngressUtil.defaultDelayMillis,
        errorRecordMultiple: Int = OptionDataIngressUtil.defaultErrorRecordMultiple,
        cost: Int = OptionDataIngressUtil.defaultCost,
        maxRandomValue: Int = OptionDataIngressUtil.defaultMaxRandomValue,
        maxBurst: Int = OptionDataIngressUtil.defaultMaxBurst)

    def parseOptions(args2: Seq[String], opts: Options): Options = args2 match {
      case Nil ⇒ opts
      case ("-h" | "--help") +: _ ⇒
        help(); sys.exit(0)
      case ("-f" | "--data-file") +: file +: tail        ⇒ parseOptions(tail, opts.copy(dataFile = file))
      case ("-d" | "--delay-millis") +: dm +: tail       ⇒ parseOptions(tail, opts.copy(delayMillis = OptionDataIngressUtil.defaultDelayMillis))
      case ("-e" | "--error-record-mult") +: erm +: tail ⇒ parseOptions(tail, opts.copy(errorRecordMultiple = erm.toInt))
      case ("-c" | "--cost") +: c +: tail                ⇒ parseOptions(tail, opts.copy(cost = c.toInt))
      case "--max-random" +: max +: tail                 ⇒ parseOptions(tail, opts.copy(maxRandomValue = max.toInt))
      case "--max-burst" +: max +: tail                  ⇒ parseOptions(tail, opts.copy(maxBurst = max.toInt))
      case which +: tail ⇒ which match {
        case ("file" | "class" | "fake" | "canned") ⇒ parseOptions(tail, opts.copy(whichOne = which))
        case unknown                                ⇒ println(s"ERROR: unknown option $unknown."); help(); sys.exit(1)
      }
    }

    val options = parseOptions(args.toSeq, Options())
    println("options: " + options)

    implicit val system = ActorSystem("CallRecordValidationSpec")
    implicit val materializer = ActorMaterializer()

    //    val util = OptionDataIngressUtil(
    //      dataFile = options.dataFile,
    //      delayMillis = options.delayMillis,
    //      errorRecordMultiple = options.errorRecordMultiple,
    //      cost = options.cost,
    //      maxRandomValue = options.maxRandomValue,
    //      maxBurst = options.maxBurst)

    val util = OptionDataIngressUtil(
      dataFile = OptionDataIngressUtil.defaultDataFile,
      delayMillis = OptionDataIngressUtil.defaultDelayMillis,
      errorRecordMultiple = OptionDataIngressUtil.defaultErrorRecordMultiple,
      cost = OptionDataIngressUtil.defaultCost,
      maxRandomValue = OptionDataIngressUtil.defaultMaxRandomValue,
      maxBurst = OptionDataIngressUtil.defaultMaxBurst)

    val lines: Source[String, NotUsed] = options.whichOne match {
      case "file"   ⇒ util.readFromFile()
      case "class"  ⇒ util.readFromClasspath()
      case "fake"   ⇒ util.fakeLines()
      case "canned" ⇒ util.cannedLines()
      case _        ⇒ println("Unknown source"); sys.exit(1)
    }
    lines.take(1000).runForeach(println)
  }
}
