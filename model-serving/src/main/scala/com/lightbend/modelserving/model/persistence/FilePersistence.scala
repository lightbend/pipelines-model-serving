package com.lightbend.modelserving.model.persistence

import java.io.{ File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.model.{ Model, ModelDescriptorUtil, ModelFactory }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import pipelinesx.logging.LoggingUtil

/**
 * Persists the state information to a file for quick recovery.
 * @param modelManager that encapsulates handling of Model I/O, etc.
 * @param baseDirPath where to write the persistent models.
 */
final case class FilePersistence[RECORD, RESULT](
    modelFactory: ModelFactory[RECORD, RESULT],
    baseDirPath:  String                       = "persistence") {

  private def getLock(fileChannel: FileChannel, shared: Boolean): (FileLock, Boolean) = {
    try {
      (fileChannel.tryLock(0L, Long.MaxValue, shared), true)
    } catch {
      case t: Throwable ⇒
        println("Error obtaining lock ")
        t.printStackTrace()
        (null, false)
    }
  }

  private def obtainLock(fileChannel: FileChannel, shared: Boolean): FileLock =
    getLock(fileChannel, shared) match {
      case lck if (lck._2) ⇒
        lck._1 match {
          case null ⇒ // retry after wait
            Thread.sleep(10)
            obtainLock(fileChannel, shared)
          case _ ⇒ // we got it
            lck._1
        }
      case _ ⇒ null
    }

  protected def fullPath(path: String): String = baseDirPath + "/" + path

  def stateExists(path: String): Boolean = {
    val file = new File(fullPath(path))
    file.exists()
  }

  // Gets an exclusive lock on the file
  // Both are returned so we can close the channels for the file...
  private def getInputStream(fileName: String): Either[String, (ObjectInputStream, FileInputStream)] = try {
    val file = new File(baseDirPath + "/" + fileName)
    val fis = new FileInputStream(file)
    val lock = obtainLock(fis.getChannel(), true)
    lock match {
      case null ⇒ Left(s"Failed to get lock for input stream for file $file")
      case _ ⇒
        val is = new ObjectInputStream(fis)
        Right(is -> fis)
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(s"getInputStream failed: Does input ${fullPath(fileName)} exist? $th")
  }

  // Both are returned so we can close the channels for the file...
  private def getOutputStream(fileName: String): Either[String, (ObjectOutputStream, FileOutputStream)] = try {
    val dir = new File(baseDirPath)
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName)
    if (!file.exists()) file.createNewFile()
    val fos = new FileOutputStream(file)
    val lock = obtainLock(fos.getChannel(), false)
    lock match {
      case null ⇒
        Left(s"Failed to get lock for output stream for file $file")
      case _ ⇒
        val os = new ObjectOutputStream(fos)
        Right(os -> fos)
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(s"getOutputStream failed: Is the ${fullPath(fileName)} location writable? $th")
  }
  /**
   * Restore the state from a file system. Use [[stateExists]] first to determine
   * if there is state to restore, as this method returns an error string if the
   * state isn't already persisted.
   * @return either an error string or the model and related data.
   */
  def restoreState(
      fileName: String): Either[String, Model[RECORD, RESULT]] =
    getInputStream(fileName) match {
      case Right((is, fis)) ⇒
        try {
          val descriptor = ModelDescriptorUtil.read(is)
          modelFactory.create(descriptor) match {
            case Right(model) ⇒ Right(model)
            case Left(error) ⇒
              Left(s"Could not restore the state for source $fileName (descriptor = ${descriptor.toRichString}). $error")
          }
        } catch {
          case t: Throwable ⇒
            val tStr = LoggingUtil.throwableToString(t)
            Left(s"Error restoring state for data type $fileName. $tStr")
        } finally {
          is.close()
          fis.getChannel.close()
        }
      case Left(error) ⇒
        Left(s"Error restoring state for data type; failed to get the input streams for data type $fileName. $error")
    }

  /**
   * Save the state to a file system.
   * @param model to persist.
   * @param filePath the location to write the state, _relative_ to the "baseDirPath".
   * @return either an error string or true.
   */
  def saveState(
      model:    Model[RECORD, RESULT],
      filePath: String): Either[String, Boolean] = {
    getOutputStream(filePath) match {
      case Right((os, fos)) ⇒
        try {
          ModelDescriptorUtil.write(model.descriptor, os)
          Right(true)
        } catch {
          case t: Throwable ⇒
            val tStr = LoggingUtil.throwableToString(t)
            Left(s"Error saving state for data type $filePath. $tStr")
        } finally {
          os.flush()
          os.close()
          fos.getChannel.close()
        }
      case Left(error) ⇒
        Left(s"Error saving state for data type $filePath. $error")
    }
  }
}
