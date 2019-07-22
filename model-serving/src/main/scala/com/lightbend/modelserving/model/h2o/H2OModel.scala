package com.lightbend.modelserving.model.h2o

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream, Serializable }
import java.util.zip.ZipInputStream

import com.lightbend.modelserving.model.{ Model, ModelMetadata, ModelType }
import hex.ModelCategory
import hex.genmodel.{ InMemoryMojoReaderBackend, MojoModel }
import hex.genmodel.easy.EasyPredictModelWrapper

import scala.collection.JavaConverters._

abstract class H2OModel[RECORD, RESULT](val metadata: ModelMetadata)
  extends Model[RECORD, RESULT] with Serializable {

  var model: EasyPredictModelWrapper = _
  setup()

  private def setup(): Unit = {
    val filesMap = scala.collection.mutable.Map[String, Array[Byte]]()
    val zis = new ZipInputStream(new ByteArrayInputStream(metadata.modelBytes))
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      val buffer = new Array[Byte](1024)
      val content = new ByteArrayOutputStream()
      Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
      filesMap += (file.getName -> content.toByteArray)
    }

    val backend = new InMemoryMojoReaderBackend(mapAsJavaMap(filesMap))
    model = new EasyPredictModelWrapper(MojoModel.load(backend))
    verifyModelType(model.getModelCategory) match {
      case true =>
      case false => throw new Exception("H2O unknown model type")
    }
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
  //     setup()
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
}
