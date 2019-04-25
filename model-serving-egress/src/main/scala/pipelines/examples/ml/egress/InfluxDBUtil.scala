package pipelines.examples.ml.egress

import java.util.Date
import java.util.concurrent.TimeUnit

import org.influxdb.{ InfluxDB, InfluxDBFactory }
import org.influxdb.dto.Point
import org.joda.time.DateTime
import pipelines.examples.data.{ Result, WineRecord }

object InfluxDBUtil {

  def write(record: Result, measurement: String, database: String, influxDB: InfluxDB): Unit = {
    val time = new Date().getTime

    val point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS)
    point.addField("result", record.result)
    point.addField("duration", record.duration)
    point.tag("model", record.name)
    write(point.build(), database, influxDB)
  }

  def write(record: WineRecord, measurement: String, database: String, influxDB: InfluxDB): Unit = {
    val time = new Date().getTime

    val point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS)
    point.addField("alchohal", record.alcohol)
    point.addField("ph", record.pH)
    point.addField("citric_acid", record.citric_acid)
    write(point.build(), database, influxDB)
  }

  private def write(point: Point, database: String, influxDB: InfluxDB): Unit = {
    try {
      influxDB.write(database, "autogen", point)
      //      influxDB.flush()
    } catch { case t: Throwable ⇒ println(s"Exception writing to Influx $t") }
  }

  def getInfluxDB(hostname: String, port: String) = {

    val url = "http://" + hostname + ":" + port

    try {
      InfluxDBFactory.connect(url)
    } catch {
      case e: Exception ⇒ {
        e.printStackTrace()
        throw e
      }
    }
  }

}
