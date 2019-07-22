package com.lightbend.modelserving.model.h2o

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable }
import java.util.zip.{ ZipEntry, ZipInputStream }

import com.lightbend.modelserving.model.{ Model, ModelMetadata, ModelType }
import hex.ModelCategory
import hex.genmodel.{ InMemoryMojoReaderBackend, MojoModel }
import hex.genmodel.easy.EasyPredictModelWrapper
import scala.collection.mutable.{ Map => MMap }

/**
 * Abstraction for all H2O models.
 * @param metadata about the model to construct. At this time, only loading the embedded "modelBytes" is supported.
 */
abstract class H2OModel[RECORD, RESULT](val metadata: ModelMetadata)
  extends Model[RECORD, RESULT] with Serializable {

  val model: EasyPredictModelWrapper = loadModel(metadata)

  protected def loadModel(metadata: ModelMetadata): EasyPredictModelWrapper = try {
    def info(msg: String): Unit = println(s"INFO: H2OModel.loadModel: $msg")

    def loadModelFiles(metadata: ModelMetadata): MMap[String, Array[Byte]] = {
      def loadFile(file: ZipEntry, zis: ZipInputStream): Array[Byte] = {
        // info(s"  Reading file file from zip archive: $file") // 2 leading spaces...
        val content = new ByteArrayOutputStream()
        val buffer = new Array[Byte](1024)
        Stream.continually(zis.read(buffer, 0, 1024))
          .takeWhile(numRead => numRead != -1)
          .foreach(numRead => content.write(buffer, 0, numRead))
        content.toByteArray
      }

      info(s"Loading model zip file from metadata: $metadata ...")
      val zis = new ZipInputStream(new ByteArrayInputStream(metadata.modelBytes))
      val filesMap = MMap.empty[String, Array[Byte]]
      Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
        filesMap += (file.getName -> loadFile(file, zis))
      }
      zis.close()
      info(s"Finished: Found files ${filesMap.keySet.toSeq.mkString(", ")}")
      filesMap
    }

    import scala.collection.JavaConverters._

    val filesMap = loadModelFiles(metadata)
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
    case scala.util.control.NonFatal(th) => throw H2OModel.H2OModelLoadError(metadata, th)
  }

  /** Abstraction for cleaning up resources */
  override def cleanup(): Unit = {}

  /** Validate model type. */
  private def verifyModelType(mc: ModelCategory): Boolean = mc match {
    case ModelCategory.Unknown => false
    case _ => true
  }

  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeObject(metadata)
  //   println(s"H2O serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   metadata = input.readObject().asInstanceOf[ModelMetadata]
  //   try {
  //     loadModel(metadata)
  //     println(s"H2O deserialization in ${System.currentTimeMillis() - start} ms")
  //   } catch {
  //     case t: Throwable ⇒
  //       throw new RuntimeException(
  //         s"H2OModel deserialization failed in ${System.currentTimeMillis() - start} ms", t)
  //   }
  // }
}

object H2OModel {
  def defaultMetadata: ModelMetadata = ModelMetadata(
    name = "H2O Model",
    description = "",
    modelType = ModelType.H2O.ordinal,
    modelBytes = Array.empty[Byte],
    location = None)

  final case class H2OModelLoadError(metadata: ModelMetadata, cause: Throwable)
    extends RuntimeException(s"H2OModel failed to load model from metadata $metadata", cause)
}
