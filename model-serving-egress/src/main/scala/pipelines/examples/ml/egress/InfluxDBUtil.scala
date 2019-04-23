package pipelines.examples.ml.egress

import java.util.concurrent.TimeUnit

import org.influxdb.{ InfluxDB, InfluxDBFactory }
import org.influxdb.dto.Point
import org.joda.time.DateTime
import pipelines.examples.data.Result

object InfluxDBUtil {

  def write(record: Result, measurement: String, database: String, influxDB: InfluxDB): Unit = {
    val dailyTempPoint = Point.measurement(measurement).time(record.time, TimeUnit.MILLISECONDS)
    dailyTempPoint.addField("result", record.result)
    dailyTempPoint.addField("duration", record.duration)
    dailyTempPoint.tag("model", record.name)
    write(dailyTempPoint.build(), database, influxDB)
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
