package pipelines.examples.data

import java.util.Date

import scala.util.control.NonFatal

object WineRecordUtil {
  def toWineRecord(array: Array[String]): Either[String, WineRecord] = {
    if (array.length != 11) {
      Left(s"Input CSV does not have 11 fields: ${arrayToString(array)}")
    } else try {
      Right(
        WineRecord(
          "WineModel",
          array(0).toDouble,
          array(1).toDouble,
          array(2).toDouble,
          array(3).toDouble,
          array(4).toDouble,
          array(5).toDouble,
          array(6).toDouble,
          array(7).toDouble,
          array(8).toDouble,
          array(9).toDouble,
          array(10).toDouble,
          0,
          new Date().getTime))
    } catch {
      case NonFatal(th) => Left(s"Failed to convert CSV to OptionRecord. CSV: ${arrayToString(array)} Error:${th.toString}")
    }
  }

  private def arrayToString(array: Array[String]): String = {
    array.mkString("[", ", ", "]")
  }

  //  def parseFromAction(action: MlAction): WineRecord = {
  //
  //  }
  //
  //  def toAction(): MlAction = {
  //
  //  }
}
