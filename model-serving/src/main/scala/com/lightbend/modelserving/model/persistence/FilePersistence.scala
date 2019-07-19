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
  private def getDataInputStream(fileName: String): Option[FileInputStream] = {
    val file = new File(baseDir + "/" + fileName)
    file.exists() match {
      case true => // File exists - try to read it
        val fis = new FileInputStream(file)
        val lock = obtainLock(fis.getChannel(), true)
        lock match {
          case null => None
          case _ => Some(fis)
        }
      case _ => None
    }
  }

  private def getDataOutputStream(fileName: String): Option[FileOutputStream] = {

    val dir = new File(baseDir)
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName)
    if (!file.exists()) file.createNewFile()
    val fos = new FileOutputStream(file)
    val lock = obtainLock(fos.getChannel(), false)
    lock match {
      case null => None
      case _ => Some(fos)
    }
  }

  def saveState(dataType: String, model: Model[_, _], name: String, description: String): Unit = getDataOutputStream(dataType) match {
    case Some(output) => // We can write to the file
      try {
        val bytes = model.toBytes
        val dos = new DataOutputStream(output)
        dos.writeUTF(name)
        dos.writeUTF(description)
        dos.writeLong(bytes.length)
        dos.writeLong(model.getType.ordinal())
        dos.write(bytes)
      } catch {
        case t: Throwable =>
          System.out.println("Error persisting model")
          t.printStackTrace()
      } finally {
        output.flush()
        output.close()
        output.getChannel.close()
      }
    case _ => // Better luck next time
  }

  def restoreState(dataType: String): Option[(Model[_, _], String, String)] = getDataInputStream(dataType) match {
    case Some(input) => // File exists - read it
      val dis = new DataInputStream(input)
      try {
        val name = dis.readUTF()
        val description = dis.readUTF()
        val length = dis.readLong.toInt
        val `type` = dis.readLong.toInt
        val bytes = new Array[Byte](length)
        dis.read(bytes)
        ModelToServe.restore(`type`, bytes) match {
          case Some(model) => Some(model, name, description)
          case _ => None
        }
      } catch {
        case t: Throwable =>
          System.out.println("Error restoring model from persistence")
          t.printStackTrace()
          None
      } finally {
        dis.close()
        input.getChannel.close()
      }
    case _ => (None)
  }
}