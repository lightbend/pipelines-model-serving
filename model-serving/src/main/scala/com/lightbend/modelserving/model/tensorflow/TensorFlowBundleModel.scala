package com.lightbend.modelserving.model.tensorflow

import java.io.File
import java.nio.file.Files
import scala.collection.mutable.{ Map ⇒ MMap }
import scala.collection.JavaConverters._

import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import com.google.protobuf.Descriptors
import org.tensorflow.{ Graph, SavedModelBundle, Session }
import org.tensorflow.framework.{ MetaGraphDef, SavedModel, SignatureDef, TensorInfo, TensorShapeProto }

/**
 * Abstract class for any TensorFlow (SavedModelBundle) model processing. It has to be extended by the user
 * implement score method, based on his own model
 * This is a very simple implementation, assuming that the TensorFlow saved model bundle is local (constructor, get tags)
 * The realistic implementation has to use some shared data storage, for example, S3, Minio, etc.
 */
abstract class TensorFlowBundleModel[RECORD, RESULT](val descriptor: ModelDescriptor)
  extends Model[RECORD, RESULT] with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  setup()
  var tags: Seq[String] = _
  var graph: Graph = _
  var signatures: Map[String, Signature] = _
  var session: Session = _

  private def setup(): Unit = {
    // Convert input into file path
    val path = new String(descriptor.modelBytes.get)
    // get tags. We assume here that the first tag is the one we use
    tags = getTags(path)
    // get saved model bundle
    val bundle = SavedModelBundle.load(path, tags(0))
    // get grapth
    graph = bundle.graph
    // get metatagraph and signature
    val metaGraphDef = MetaGraphDef.parseFrom(bundle.metaGraphDef)
    val signatureMap = metaGraphDef.getSignatureDefMap.asScala
    //  parse signature, so that we can use definitions (if necessary) programmatically in score method
    signatures = parseSignatures(signatureMap)
    // Create TensorFlow session
    session = bundle.session
  }

  override def cleanup(): Unit = {
    try {
      session.close
    } catch {
      case t: Throwable ⇒
        println(s"WARNING: in TensorFlowBundleModel.cleanup(), call to session.close threw $t. Ignoring")
    }
    try {
      graph.close
    } catch {
      case t: Throwable ⇒
        println(s"WARNING: in TensorFlowBundleModel.cleanup(), call to graph.close threw $t. Ignoring")
    }
  }

  // TODO: Verify if these methods are actually needed, since they have only one field,
  // the descriptor, which has these methods:
  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeObject(descriptor)
  //   println(s"TensorFlow bundled serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   descriptor = input.readObject().asInstanceOf[ModelDescriptor]
  //   try {
  //     setup()
  //     println(s"TensorFlow bundled deserialization in ${System.currentTimeMillis() - start} ms")
  //   } catch {
  //     case t: Throwable =>
  //       throw new RuntimeException(
  //         s"TensorFlow bundled deserialization failed in ${System.currentTimeMillis() - start} ms", t)
  //   }
  // }

  private def parseSignatures(signatures: MMap[String, SignatureDef]): Map[String, Signature] = {
    signatures.map(signature ⇒
      signature._1 -> Signature(parseInputOutput(signature._2.getInputsMap.asScala), parseInputOutput(signature._2.getOutputsMap.asScala))).toMap
  }

  private def parseInputOutput(inputOutputs: MMap[String, TensorInfo]): Map[String, Field] =
    inputOutputs.map {
      case (key, info) ⇒
        var name = ""
        var dtype: Descriptors.EnumValueDescriptor = null
        var shape = Seq.empty[Int]
        info.getAllFields.asScala.foreach { descriptor ⇒
          if (descriptor._1.getName.contains("shape")) {
            descriptor._2.asInstanceOf[TensorShapeProto].getDimList.toArray.map(d ⇒
              d.asInstanceOf[TensorShapeProto.Dim].getSize).toSeq.foreach(v ⇒ shape = shape :+ v.toInt)

          }
          if (descriptor._1.getName.contains("name")) {
            name = descriptor._2.toString.split(":")(0)
          }
          if (descriptor._1.getName.contains("dtype")) {
            dtype = descriptor._2.asInstanceOf[Descriptors.EnumValueDescriptor]
          }
        }
        key -> Field(name, dtype, shape)
    }.toMap

  // This method gets all tags in the saved bundle and uses the first one. If you need a specific tag, overwrite this method
  // With a seq (of one) tags returning desired tag.
  protected def getTags(directory: String): Seq[String] = {
    val d = new File(directory)
    val pbfiles = if (d.exists && d.isDirectory)
      d.listFiles.filter(_.isFile).filter(name ⇒ (name.getName.endsWith("pb") || name.getName.endsWith("pbtxt"))).toList
    else List[File]()
    if (pbfiles.length > 0) {
      val byteArray = Files.readAllBytes(pbfiles(0).toPath)
      SavedModel.parseFrom(byteArray).getMetaGraphsList.asScala.
        flatMap(graph ⇒ graph.getMetaInfoDef.getTagsList.asByteStringList.asScala.map(_.toStringUtf8))
    } else {
      Seq.empty
    }
  }
}

/** Definition of the field (input/output) */
case class Field(name: String, `type`: Descriptors.EnumValueDescriptor, shape: Seq[Int])

/** Definition of the signature */
case class Signature(inputs: Map[String, Field], outputs: Map[String, Field])
