package com.lightbend.modelserving.model.persistence

import java.io.{ File, FileInputStream, FileOutputStream, DataInputStream, DataOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.model.{ Model, ModelDescriptorUtil, ModelFactory }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import pipelinesx.logging.LoggingUtil

/**
 * Persists the state information to a file for quick recovery.
 * @param modelManager that encapsulates handling of Model I/O, etc.
 * @param baseDirPath where to write the persistent models. TODO: relying on the global variable for the default is error prone.
 */
final case class FilePersistence[RECORD, RESULT](
    modelFactory: ModelFactory[RECORD, RESULT],
    baseDirPath:  String                       = FilePersistence.mountPointRoot) {

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

  def statePath(path: String): String = baseDirPath + "/" + path

  def stateExists(path: String): Boolean = {
    val file = new File(statePath(path))
    file.exists()
  }

  // Gets an exclusive lock on the file
  // Both are returned so we can close the channels for the file...
  private def getInputStream(fileName: String): Either[String, (DataInputStream, FileInputStream)] = try {
    val file = new File(statePath(fileName))
    val fis = new FileInputStream(file)
    val lock = obtainLock(fis.getChannel(), true)
    lock match {
      case null ⇒ Left(s"Failed to get lock for input stream for file $file")
      case _ ⇒
        val is = new DataInputStream(fis)
        Right(is -> fis)
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(throwableMsg(s"getInputStream failed: Does input ${statePath(fileName)} exist?", th))
  }

  // Both are returned so we can close the channels for the file...
  private def getOutputStream(fileName: String): Either[String, (DataOutputStream, FileOutputStream)] = try {
    val dir = new File(baseDirPath)
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName)
    val fileDir = file.getParentFile()
    // make sure all the parent directories exist.
    if (dir.exists() || fileDir.mkdirs()) {
      if (!file.exists()) file.createNewFile()
      val fos = new FileOutputStream(file)
      val lock = obtainLock(fos.getChannel(), false)
      lock match {
        case null ⇒
          Left(s"Failed to get lock for output stream for file $file")
        case _ ⇒
          val os = new DataOutputStream(fos)
          Right(os -> fos)
      }
    } else {
      Left(s"Could not create the parent directories ($fileDir) for file: $file")
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(throwableMsg(s"getOutputStream failed: Is the ${statePath(fileName)} location writable?", th))
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
            Left(throwableMsg(s"Error restoring state for data type $fileName.", t))
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
            Left(throwableMsg(s"Error saving state for data type $filePath.", t))
        } finally {
          os.flush()
          os.close()
          fos.getChannel.close()
        }
      case Left(error) ⇒
        Left(s"Error saving state for data type $filePath. $error")
    }
  }

  private def throwableMsg(msg: String, th: Throwable): String =
    msg + " " + LoggingUtil.throwableToString(th)
}

object FilePersistence {
  var mountPointRoot: String = "persistence"

  def setGlobalMountPoint(mount: String): Unit = {
    mountPointRoot = mount + "/persistence"
  }

  def apply[RECORD, RESULT](modelFactory: ModelFactory[RECORD, RESULT]): FilePersistence[RECORD, RESULT] =
    new FilePersistence[RECORD, RESULT](modelFactory, mountPointRoot)

}
