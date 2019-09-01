package com.lightbend.modelserving.model.persistence

import java.io.{ File, FileInputStream, FileOutputStream, DataInputStream, DataOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.model.{ Model, ModelDescriptorUtil, ModelFactory }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import pipelinesx.logging.LoggingUtil

/**
 * Persists the model state information to a file for quick recovery.
 * @param modelName unique name of the model, used in the file name for persistence.
 * @param modelFactory that encapsulates handling of Model I/O, etc.
 * @param baseDirPath where to write the persistent models. It will be created if necessary.
 */
final case class ModelPersistence[RECORD, RESULT](
    modelName:    String,
    modelFactory: ModelFactory[RECORD, RESULT],
    baseDirPath:  File) {

  private val fileName: String = s"${modelName}_model.dat"

  val fullPath: File = new File(baseDirPath, fileName)

  /** Is there a previously-saved state snapshot? */
  def stateExists(): Boolean = fullPath.exists()

  /**
   * Delete a previously-saved state snapshot, if any.
   * @return true if deletion successful or there wasn't a previous snapshot, but return false if a snapshot exists, but couldn't be deleted.
   */
  def clean(): Boolean = stateExists() == false || fullPath.delete()

  /**
   * Restore the state from a file system. Use [[stateExists]] first to determine
   * if there is state to restore, as this method returns an error string if the
   * state isn't already persisted.
   * @return either an error string or the model and related data.
   */
  def restoreState(): Either[String, Model[RECORD, RESULT]] =
    getInputStream() match {
      case Right((is, fis)) ⇒
        try {
          val descriptor = ModelDescriptorUtil.read(is)
          modelFactory.create(descriptor) match {
            case Right(model) ⇒ Right(model)
            case Left(error) ⇒
              Left(s"Could not restore the model state from $fullPath (descriptor = ${descriptor.toRichString}). $error")
          }
        } catch {
          case t: Throwable ⇒
            Left(throwableMsg(s"Error restoring the model state from $fullPath.", t))
        } finally {
          is.close()
          fis.getChannel.close()
        }
      case Left(error) ⇒
        Left(s"Error restoring model state from $fullPath; failed to get the input streams for the file. $error")
    }

  /**
   * Save the state to a file system.
   * @param model to persist.
   * @return either an error string or true.
   */
  def saveState(model: Model[RECORD, RESULT]): Either[String, Boolean] = {
    getOutputStream() match {
      case Right((os, fos)) ⇒
        try {
          ModelDescriptorUtil.write(model.descriptor, os)
          Right(true)
        } catch {
          case t: Throwable ⇒
            Left(throwableMsg(s"Error saving model state to $fullPath.", t))
        } finally {
          os.flush()
          os.close()
          fos.getChannel.close()
        }
      case Left(error) ⇒
        Left(s"Error saving model state to $fullPath. $error")
    }
  }

  private def throwableMsg(msg: String, th: Throwable): String =
    msg + " " + LoggingUtil.throwableToString(th)

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
      case lck if lck._2 ⇒
        lck._1 match {
          case null ⇒ // retry after wait
            Thread.sleep(10)
            obtainLock(fileChannel, shared)
          case _ ⇒ // we got it
            lck._1
        }
      case _ ⇒ null
    }

  // Gets an exclusive lock on the file
  // Both are returned so we can close the channels for the file...
  private def getInputStream(): Either[String, (DataInputStream, FileInputStream)] = try {
    if (stateExists() == false) Left(s"Model file $fullPath does not exist!")
    else {
      val fis = new FileInputStream(fullPath)
      val lock = obtainLock(fis.getChannel(), true)
      lock match {
        case null ⇒ Left(s"Failed to get lock for input stream for model file $fullPath. If here, it appears to exist; is it readable?")
        case _ ⇒
          val is = new DataInputStream(fis)
          Right(is -> fis)
      }
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(throwableMsg(s"getInputStream failed: for model input $fullPath", th))
  }

  // Both are returned so we can close the channels for the file...
  private def getOutputStream(): Either[String, (DataOutputStream, FileOutputStream)] = try {
    // Make sure all the parent directories (possibly more than one) exist.
    if (!baseDirPath.exists()) baseDirPath.mkdirs()
    if (!baseDirPath.exists()) Left(s"Failed to create the parent directories for the output model file, $fullPath")
    else {
      // Make sure the file exist.
      if (!fullPath.exists()) fullPath.createNewFile()
      if (!fullPath.exists()) Left(s"Failed to create file for output model, $fullPath")
      else {
        val fos = new FileOutputStream(fullPath)
        val lock = obtainLock(fos.getChannel(), false)
        lock match {
          case null ⇒
            Left(s"Failed to get lock for output stream for model file $fullPath")
          case _ ⇒
            val os = new DataOutputStream(fos)
            Right(os -> fos)
        }
      }
    }
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(throwableMsg(s"getOutputStream failed: Is the ${fullPath} location writable?", th))
  }
}
