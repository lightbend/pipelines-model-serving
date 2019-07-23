package com.lightbend.modelserving.model.h2o

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, Serializable }
import java.util.zip.ZipInputStream
import scala.collection.mutable.{ Map ⇒ MMap }

import com.lightbend.modelserving.model.{ Model, ModelDescriptor, ModelType }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import hex.ModelCategory
import hex.genmodel.{ InMemoryMojoReaderBackend, MojoModel }
import hex.genmodel.easy.EasyPredictModelWrapper

/**
 * Abstraction for all H2O models.
 * @param descriptor about the model to construct. At this time, only loading the embedded "modelBytes" is supported.
 */
abstract class H2OModel[RECORD, RESULT](val descriptor: ModelDescriptor)
  extends Model[RECORD, RESULT] with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  protected def loadModel(descriptor: ModelDescriptor): EasyPredictModelWrapper = try {
      def info(msg: String): Unit = println(s"INFO: H2OModel.loadModel: $msg")

      def loadModelFiles(descriptor: ModelDescriptor): MMap[String, Array[Byte]] = {
          def loadFile(zis: ZipInputStream): Array[Byte] = {
            val content = new ByteArrayOutputStream()
            val buffer = new Array[Byte](1024)
            Stream.continually(zis.read(buffer, 0, 1024))
              .takeWhile(numRead ⇒ numRead != -1)
              .foreach(numRead ⇒ content.write(buffer, 0, numRead))
            content.toByteArray
          }

        info(s"Loading model zip file from descriptor: ${descriptor.toRichString} ...")
        val zis = new ZipInputStream(new ByteArrayInputStream(descriptor.modelBytes.get))
        val filesMap = MMap.empty[String, Array[Byte]]
        Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file ⇒
          // info(s"  Reading file file from zip archive: $file") // 2 leading spaces...
          filesMap += (file.getName -> loadFile(zis))
        }
        zis.close()
        info(s"Finished: Found files ${filesMap.keySet.toSeq.mkString(", ")}")
        filesMap
      }

    import scala.collection.JavaConverters._

    val filesMap = loadModelFiles(descriptor)
    val backend = new InMemoryMojoReaderBackend(mapAsJavaMap(filesMap))
    val m = new EasyPredictModelWrapper(MojoModel.load(backend))
    val cat = m.getModelCategory
    if (verifyModelType(cat) == false) {
      throw new RuntimeException(s"Unknown H2O model category: $cat")
    } else {
      info(s"Successfully loaded H2O model, category = $cat")
    }
    m
  } catch {
    case scala.util.control.NonFatal(th) ⇒ throw H2OModel.H2OModelLoadError(descriptor, th)
  }

  val model: EasyPredictModelWrapper = loadModel(descriptor)

  /** Abstraction for cleaning up resources */
  override def cleanup(): Unit = {}

  /** Validate model type. */
  private def verifyModelType(mc: ModelCategory): Boolean = mc match {
    case ModelCategory.Unknown ⇒ false
    case _                     ⇒ true
  }

  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeObject(descriptor)
  //   println(s"H2O serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   descriptor = input.readObject().asInstanceOf[ModelDescriptor]
  //   try {
  //     loadModel(descriptor)
  //     println(s"H2O deserialization in ${System.currentTimeMillis() - start} ms")
  //   } catch {
  //     case t: Throwable ⇒
  //       throw new RuntimeException(
  //         s"H2OModel deserialization failed in ${System.currentTimeMillis() - start} ms", t)
  //   }
  // }
}

object H2OModel {
  def defaultDescriptor: ModelDescriptor = ModelDescriptor(
    name = "H2O Model",
    description = "",
    dataType = "",
    modelType = ModelType.H2O,
    modelBytes = None,
    modelSourceLocation = None)

  final case class H2OModelLoadError(descriptor: ModelDescriptor, cause: Throwable)
    extends RuntimeException(s"H2OModel failed to load model from descriptor ${descriptor.toRichString}", cause)
}
