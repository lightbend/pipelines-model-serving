package pipelines.examples.modelserving

import pipelines.akkastream.{ AkkaStreamlet, StreamletLogic }
import pipelines.examples.data._
import pipelines.examples.data.DataCodecs._

import pipelines.streamlets.{ FanIn, _ }

import hex.genmodel.MojoModel
import hex.genmodel.easy.EasyPredictModelWrapper
import hex.genmodel.easy.RowData

import java.io.{ File, FileOutputStream }

class AirlineFlightModelServerStreamlet extends AkkaStreamlet {

  override implicit val shape = new AirlineFlightFanInOut[AirlineFlightRecord, AirlineFlightResult]

  override final def createLogic = new StreamletLogic {
    // (implicit shape: AirlineFlightFanInOut[AirlineFlightRecord, AirlineFlightResult], context: StreamletContext) extends
    val server = new AirlineFlightModelServer()

    val in = atLeastOnceSource[AirlineFlightRecord](shape.inlet0)
    val out = atLeastOnceSink[AirlineFlightResult](shape.outlet0)

    override def init(): Unit = {
      in.map(record ⇒ server.score(record)).runWith(out)
    }
  }
}

object AirlineFlightFanInOut {
  val InletName = new IndexedPrefix("in", 2)
  val outletName = new IndexedPrefix("out", 1)
}

final class AirlineFlightFanInOut[In0: KeyedSchema, Out0: KeyedSchema] extends StreamletShape {
  val inlet0 = KeyedInletPort[In0](FanIn.inletName(0))
  val outlet0 = KeyedOutletPort[Out0](FanOut.outletName(0))

  final override def inlets = Vector(inlet0)
  final override def outlets = Vector(outlet0)
}

class AirlineFlightModelServer() {

  protected lazy val config = com.typesafe.config.ConfigFactory.load()
  protected lazy val currentModel: EasyPredictModelWrapper = getModel()

  // Hack: We load the file from a resource on the class path, write it to a temp.
  // file locally, then use the Mojo API to load _that_ file. A real implementation
  // would read the file directly from a PVC-mounted file system instead, which also
  // is required for periodic updates.
  // Also, for this implementation, we only read one model in the list of models...
  protected def getModel(): EasyPredictModelWrapper = {
    val tmpFile = "/tmp/mojo.zip"
    lazy val resource: String =
      config.getStringList("airline-flights.model-sources").get(0)
    val classloader = Thread.currentThread().getContextClassLoader()
    val is = classloader.getResourceAsStream(resource)
    val os = new FileOutputStream(new File(tmpFile))
    val n = 1024 * 1024
    val buffer = Array.fill[Byte](n)(0)
    println("Writing model zip file bytes...")
    var actual = is.read(buffer, 0, n)
    while (actual > 0) {
      print(s"$actual ")
      os.write(buffer, 0, actual)
      actual = is.read(buffer, 0, n)
    }
    println()
    is.close()
    os.close()

    new EasyPredictModelWrapper(MojoModel.load(tmpFile))
  }

  def toRow(record: AirlineFlightRecord): RowData = {
    val row = new RowData
    row.put("Year", record.year.toString)
    row.put("Month", record.month.toString)
    row.put("DayofMonth", record.dayOfMonth.toString) // note spelling...
    row.put("DayOfWeek", record.dayOfWeek.toString)
    row.put("CRSDepTime", record.crsDepTime.toString) // spelling...
    row.put("UniqueCarrier", record.uniqueCarrier.toString)
    row.put("Origin", record.origin.toString)
    row.put("Dest", record.destination.toString) // spelling...
    row
  }

  def toResult(record: AirlineFlightRecord, label: String, probability: Double): AirlineFlightResult =
    AirlineFlightResult(
      year = record.year,
      month = record.month,
      dayOfMonth = record.dayOfMonth,
      dayOfWeek = record.dayOfWeek,
      depTime = record.depTime,
      arrTime = record.arrTime,
      uniqueCarrier = record.uniqueCarrier,
      flightNum = record.flightNum,
      arrDelay = record.arrDelay,
      depDelay = record.depDelay,
      origin = record.origin,
      destination = record.destination,
      delayPredictionLabel = label,
      delayPredictionProbability = probability)

  def score(record: AirlineFlightRecord): AirlineFlightResult = {
    val row = toRow(record)
    val prediction = currentModel.predictBinomial(row)
    val probs = prediction.classProbabilities
    val probability = if (probs.length == 2) probs(1) else 0.0
    println(s"Prediction that flight departure will be delayed: ${prediction.label} (probability: $probability) for $record")
    toResult(record, prediction.label, probability)
  }
}

