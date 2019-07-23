package pipelinesx.egress.influxdb

import java.util.Date
import java.util.concurrent.TimeUnit

import org.influxdb.{ InfluxDB, InfluxDBFactory }
import org.influxdb.dto.Point

object InfluxDBUtil {

  trait Writer[R] {
    def write(record: R, measurement: String, database: String, influxDB: InfluxDB): Unit = {
      val time = new Date().getTime
      val point = {
        val p = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS)
        addFields(p, record)
      }
      doWrite(point.build(), database, influxDB)
    }

    protected def doWrite(point: Point, database: String, influxDB: InfluxDB): Unit =
      try {
        influxDB.write(database, "autogen", point)
        //      influxDB.flush()
      } catch {
        case scala.util.control.NonFatal(th) ⇒ println(s"Exception writing to InfluxDB database $database: $th")
      }

    def addFields(point: Point.Builder, record: R): Point.Builder
  }

  def getInfluxDB(hostname: String, port: Int) = {

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
