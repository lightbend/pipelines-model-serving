package pipelines.examples.modelserving.airlineflights

import pipelines.akkastream.AkkaStreamlet
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.examples.modelserving.airlineflights.data._
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }
import pipelines.streamlets.StreamletShape

import hex.genmodel.MojoModel
import hex.genmodel.easy.EasyPredictModelWrapper
import hex.genmodel.easy.RowData

import java.io.{ File, FileOutputStream }

final case object AirlineFlightModelDeanServer extends AkkaStreamlet {

  val in = AvroInlet[AirlineFlightRecord]("in")
  val out = AvroOutlet[AirlineFlightResult]("out", r ⇒ r.uniqueCarrier)

  final override val shape = StreamletShape.withInlets(in).withOutlets(out)

  override final def createLogic = new RunnableGraphStreamletLogic() {
    val server = new AirlineFlightModelDeanServer()
    def runnableGraph() = {
      atLeastOnceSource(in).map(record ⇒ server.score(record)).to(atLeastOnceSink(out))
    }
  }
}

class AirlineFlightModelDeanServerUtil() {

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
      delayPredictionProbability = probability,
      modelname = "",
      dataType =  "",
      duration = 0)

  // We cheat a bit; on errors, we stuff the error string in the "prediction" string
  // and still pretend it's normal.
  // TODO: write these records to a separate error egress.
  def score(record: AirlineFlightRecord): AirlineFlightResult = {
    val row = toRow(record)
    try {
      val prediction = currentModel.predictBinomial(row)
      val probs = prediction.classProbabilities
      val probability = if (probs.length == 2) probs(1) else 0.0
      println(s"Prediction that flight departure will be delayed: ${prediction.label} (probability: $probability) for $record")
      toResult(record, prediction.label, probability)
    } catch {
      case util.control.NonFatal(ex) ⇒
        println(s"""ERROR: Exception "$ex" while scoring record $record""")
        toResult(record, ex.getMessage(), 0.0)
    }
  }
}

