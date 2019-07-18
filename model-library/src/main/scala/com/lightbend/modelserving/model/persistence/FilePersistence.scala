package com.lightbend.modelserving.model.persistence

import java.io.{ DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.model.{ Model, ModelToServe }

/**
 * Persists the state information to a file for quick recovery.
 */

object FilePersistence {

  private final val baseDir = "persistence"

  private def getLock(fileChannel: FileChannel): (FileLock, Boolean) = {
    try {
      (fileChannel.tryLock(), true)
    } catch {
      case t: Throwable =>
        System.out.println("Error obtaining lock")
        t.printStackTrace()
        (null, false)
    }
  }

  private def obtainLock(fileChannel: FileChannel): FileLock = getLock(fileChannel) match {
    case lck if (lck._2) =>
      lck._1 match {
        case null => // retry after wait
          Thread.sleep(10)
          obtainLock(fileChannel)
        case _ => // we got it
          lck._1
      }
    case _ => null
  }

  // Gets ByteBuffer with exlsive lock on the file
  private def getDataInputStream(fileName: String): Option[(DataInputStream, FileLock)] = {
    val file = new File(baseDir + "/" + fileName)
    file.exists() match {
      case true => // File exists - try to read it
        val fis = new FileInputStream(file)
        val lock = obtainLock(fis.getChannel())
        lock match {
          case null => None
          case _ => Some((new DataInputStream(fis), lock))
        }
      case _ => None
    }
  }

  private def getDataOutputStream(fileName: String): Option[(DataOutputStream, FileLock)] = {

    val dir = new File(baseDir)
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName)
    if (!file.exists()) file.createNewFile()
    val fos = new FileOutputStream(file)
    val lock = obtainLock(fos.getChannel())
    lock match {
      case null => None
      case _ => Some(new DataOutputStream(fos), lock)
    }
  }

  def saveState(dataType: String, model: Model[_, _]): Unit = getDataOutputStream(dataType) match {
    case Some(output) => // We can write to the file
      try {
        val bytes = model.toBytes
        output._1.writeLong(bytes.length)
        output._1.writeLong(model.getType.ordinal())
        output._1.write(bytes)
      } catch {
        case t: Throwable =>
          System.out.println("Error persisting model")
          t.printStackTrace()
      } finally {
        output._2.release()
        output._1.flush()
        output._1.close()
      }
    case _ => // Better luck next time
  }

  def restoreState(dataType: String): Option[Model[_, _]] = getDataInputStream(dataType) match {
    case Some(input) => // File exists - read it
      try {
        val length = input._1.readLong.toInt
        val `type` = input._1.readLong.toInt
        val bytes = new Array[Byte](length)
        input._1.read(bytes)
        ModelToServe.restore(`type`, bytes)
      } catch {
        case t: Throwable =>
          System.out.println("Error restoring model from persistence")
          t.printStackTrace()
          None
      } finally {
        input._2.release()
        input._1.close()
      }
    case _ => (None)
  }
}