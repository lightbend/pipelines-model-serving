package com.lightbend.modelserving.model.persistence

import java.io.{ DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.model.{ Model, ModelToServe }

/**
 * Persists the state information to a file for quick recovery.
 */

object FilePersistence {

  private final val baseDir = "persistence"

  private def getLock(fileChannel: FileChannel, shared: Boolean): (FileLock, Boolean) = {
    try {
      (fileChannel.tryLock(0L, Long.MaxValue, shared), true)
    } catch {
      case t: Throwable =>
        println("Error obtaining lock ")
        t.printStackTrace()
        (null, false)
    }
  }

  private def obtainLock(fileChannel: FileChannel, shared: Boolean): FileLock = getLock(fileChannel, shared) match {
    case lck if (lck._2) =>
      lck._1 match {
        case null => // retry after wait
          Thread.sleep(10)
          obtainLock(fileChannel, shared)
        case _ => // we got it
          lck._1
      }
    case _ => null
  }

  // Gets ByteBuffer with exlsive lock on the file
  private def getDataInputStream(fileName: String): Either[String, FileInputStream] = {
    val file = new File(baseDir + "/" + fileName)
    if (file.exists()) {
      val fis = new FileInputStream(file)
      val lock = obtainLock(fis.getChannel(), true)
      lock match {
        case null => Left(s"Failed to get lock for input stream for file $file")
        case _ => Right(fis)
      }
    } else {
      Left(s"getDataInputStream: $file doesn't exist! (basedir = $baseDir, fileName = $fileName)")
    }
  }

  private def getDataOutputStream(fileName: String): Either[String, FileOutputStream] = {

    val dir = new File(baseDir)
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName)
    if (!file.exists()) file.createNewFile()
    val fos = new FileOutputStream(file)
    val lock = obtainLock(fos.getChannel(), false)
    lock match {
      case null => Left(s"Failed to get lock for output stream for file $file")
      case _ => Right(fos)
    }
  }

  /**
   * Save the state to a file system.
   * @return either an error string or true.
   */
  def saveState[RECORD, RESULT](
    dataType: String,
    model: Model[RECORD, RESULT],
    name: String,
    description: String): Either[String, Boolean] =
    getDataOutputStream(dataType) match {
      case Right(output) =>
        try {
          val bytes = model.toBytes
          val dos = new DataOutputStream(output)
          dos.writeUTF(name)
          dos.writeUTF(description)
          dos.writeLong(bytes.length)
          dos.writeLong(model.getType.ordinal())
          dos.write(bytes)
          Right(true)
        } catch {
          case t: Throwable =>
            Left(s"Error saving state for data type $dataType, name $name, description $description. $t" + formatStackTrace(t))
        } finally {
          output.flush()
          output.close()
          output.getChannel.close()
        }
      case Left(error) =>
        Left(s"Error saving state for data type $dataType, name $name, description $description. $error")
    }

  /**
   * Restore the state from a file system.
   * @return either an error string or the model and related data.
   */
  def restoreState[RECORD, RESULT](
    dataType: String): Either[String, (Model[RECORD, RESULT], String, String)] =
    getDataInputStream(dataType) match {
      case Right(input) =>
        val dis = new DataInputStream(input)
        try {
          val name = dis.readUTF()
          val description = dis.readUTF()
          val length = dis.readLong.toInt
          val modelType = dis.readLong.toInt
          val bytes = new Array[Byte](length)
          dis.read(bytes)
          ModelToServe.restore[RECORD, RESULT](modelType, bytes) match {
            case Right(model) => Right((model, name, description))
            case Left(error) =>
              Left(s"Could not restore the state for dataType $dataType (name = $name, description = $description, length = $length, modelType = $modelType). $error")
          }
        } catch {
          case t: Throwable =>
            Left(s"Error restoring state for data type $dataType. $t" + formatStackTrace(t))
        } finally {
          dis.close()
          input.getChannel.close()
        }
      case Left(error) =>
        Left(s"Error restoring state for data type; failed to get the input streams for data type $dataType. $error")
    }

  private def formatStackTrace(th: Throwable): String = th.getStackTrace().mkString("\n  ", "\n  ", "\n")
}
