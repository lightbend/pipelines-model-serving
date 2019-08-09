package com.lightbend.modelserving.model.persistence

import java.io.{ DataInputStream, DataOutputStream, File, FileInputStream, FileOutputStream }
import java.nio.channels.{ FileChannel, FileLock }

import com.lightbend.modelserving.merger.StreamMerger
import com.lightbend.modelserving.model.{ Model, ModelDescriptorUtil, ModelFactory }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import com.lightbend.modelserving.splitter.{ OutputPercentage, StreamSplitter }
import pipelinesx.logging.LoggingUtil

import scala.collection.mutable.ListBuffer

/**
 * Persists the state information to a file for quick recovery.
 * @param modelFactory that encapsulates handling of Model I/O, etc.
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

  def fileName(path: String): String = s"${FilePersistence.streamlet}_$path"
  def statePath(path: String): String = s"$baseDirPath /${fileName(path)}"

  def stateExists(path: String): Boolean = {
    val file = new File(statePath(path))
    file.exists()
  }

  // Gets an exclusive lock on the file
  // Both are returned so we can close the channels for the file...
  private def getInputStream(filename: String): Either[String, (DataInputStream, FileInputStream)] = try {
    val file = new File(statePath(filename))
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
      Left(throwableMsg(s"getInputStream failed: Does input ${statePath(filename)} exist?", th))
  }

  // Both are returned so we can close the channels for the file...
  private def getOutputStream(filename: String): Either[String, (DataOutputStream, FileOutputStream)] = try {
    val dir = new File(baseDirPath)
    // make sure all the parent directories exist.
    if (!dir.exists()) dir.mkdir()
    val file = new File(dir, fileName(filename))
    // make sure the file exist.
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
  } catch {
    case scala.util.control.NonFatal(th) ⇒
      Left(throwableMsg(s"getOutputStream failed: Is the ${statePath(filename)} location writable?", th))
  }
  /**
   * Restore the model server state from a file system. Use [[stateExists]] first to determine
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
   * Restore the state of a splitter from a file system. Use [[stateExists]] first to determine
   * if there is state to restore, as this method returns an error string if the
   * state isn't already persisted.
   * @return either an error string or the splitter.
   */
  def restoreSplitState(
      fileName: String): Either[String, StreamSplitter] = getInputStream(fileName) match {
    case Right((is, fis)) ⇒
      try {
        val ninputs = is.readLong().toInt
        val inputs = new ListBuffer[OutputPercentage]()
        0 to ninputs - 1 foreach (i ⇒ {
          val output = is.readLong().toInt
          val percentage = is.readLong().toInt
          inputs += new OutputPercentage(output, percentage)
        })
        Right(new StreamSplitter(inputs))
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
   * Restore the state of a merger from a file system. Use [[stateExists]] first to determine
   * if there is state to restore, as this method returns an error string if the
   * state isn't already persisted.
   * @return either an error string or the merger.
   */
  def restoreMergerState(
      fileName: String): Either[String, StreamMerger] = getInputStream(fileName) match {
    case Right((is, fis)) ⇒
      try {
        val tmout = is.readLong()
        Right(new StreamMerger(tmout))
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
   * Save the state of the model server to a file system.
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

  /**
   * Save the state of a splitter to a file system.
   * @param splitter to persist.
   * @param filePath the location to write the state, _relative_ to the "baseDirPath".
   * @return either an error string or true.
   */
  def saveState(
      splitter: StreamSplitter,
      filePath: String): Either[String, Boolean] = {
    getOutputStream(filePath) match {
      case Right((os, fos)) ⇒
        try {
          os.writeLong(splitter.split.size.toLong)
          splitter.split.foreach(record ⇒ {
            os.writeLong(record.output.toLong)
            os.writeLong(record.percentage.toLong)
          })

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

  /**
   * Save the state of a merger to a file system.
   * @param merger to persist.
   * @param filePath the location to write the state, _relative_ to the "baseDirPath".
   * @return either an error string or true.
   */
  def saveState(
      merger:   StreamMerger,
      filePath: String): Either[String, Boolean] = {
    getOutputStream(filePath) match {
      case Right((os, fos)) ⇒
        try {
          os.writeLong(merger.timeout)
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

  private def throwableMsg(msg: String, th: Throwable): String = msg + " " + LoggingUtil.throwableToString(th)

}

object FilePersistence {
  var mountPointRoot: String = "persistence"
  var streamlet: String = ""

  def setGlobalMountPoint(mount: String): Unit = {
    mountPointRoot = mount + "/persistence"
  }

  def setStreamletName(name: String): Unit = {
    streamlet = name
  }
}
