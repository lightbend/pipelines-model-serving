package com.lightbend.modelserving.model.tensorflow

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.zip.{ZipEntry, ZipInputStream, ZipOutputStream}

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._
import com.lightbend.modelserving.model.{ModelBase, ModelDescriptor}
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._
import com.google.protobuf.Descriptors
import org.tensorflow.SavedModelBundle
import org.tensorflow.framework.{MetaGraphDef, SavedModel, SignatureDef, TensorInfo, TensorShapeProto}

/**
 * Abstract class for any TensorFlow (SavedModelBundle) model processing. It has to be extended by the user
 * implement score method, based on his own model
 * This is a very simple implementation, assuming that the TensorFlow saved model bundle is local (constructor, get tags)
 * The realistic implementation has to use some shared data storage, for example, S3, Minio, etc.
 */
abstract class TensorFlowBundleModel[RECORD, MODEL_OUTPUT](descriptor: ModelDescriptor)(makeDefaultModelOutput: () ⇒ MODEL_OUTPUT)
  extends ModelBase[RECORD, MODEL_OUTPUT](descriptor)(makeDefaultModelOutput) with Serializable {

  assert(descriptor.modelSourceLocation != None || descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  type Signatures = Map[String, Signature]
  val unzipDirectory = "tmp/model"
  // Convert input into file path
  val path = descriptor.modelSourceLocation match {
    case Some(location) => location
    case _ => Zipper.unzipMessage(descriptor.modelBytes.get, unzipDirectory)
  }
  // get tags. We assume here that the first tag is the one we use
  val tags: Seq[String] = getTags(path)
  val bundle = SavedModelBundle.load(path, tags(0))
  val graph = bundle.graph
  // get metatagraph and signature
  val metaGraphDef = MetaGraphDef.parseFrom(bundle.metaGraphDef)
  val signatureMap = metaGraphDef.getSignatureDefMap.asScala
  //  parse signature, so that we can use definitions (if necessary) programmatically in score method
  val signatures = parseSignatures(signatureMap)
  // Create TensorFlow session
  val session = bundle.session

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

// Support class encapsulating zipping operations
object Zipper{
  // Recursively delete files in directory
  private def deleteRecursively(file: File): Unit = {
    if(file.exists) {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      val _ = file.delete
    }
  }

  // delete directory content
  private def deleteDirectoryContent(file: File): Unit = {
    if(file.exists) {
      if (file.isDirectory)
        file.listFiles.foreach(deleteRecursively)
    }
  }

  // Unzip message into directory
  def unzipMessage(data: Array[Byte], directory : String) : String = {
    val destination = new File(directory)
    deleteDirectoryContent(destination)
    val zis = new ZipInputStream(new ByteArrayInputStream(data))
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).filter(!_.isDirectory).foreach ( entry => {
      //      println(s"Unzipping file ${entry.getName}")
      val outPath = destination.toPath.resolve(entry.getName)
      val outPathParent = outPath.getParent
      if (!outPathParent.toFile.exists()) {
        outPathParent.toFile.mkdirs()
      }
      val outFile = outPath.toFile
      val out = new FileOutputStream(outFile)
      val buffer = new Array[Byte](4096)
      Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
      out.close()
    })
    destination.listFiles(_.isDirectory).head.getAbsolutePath
  }

  // recursively zip content of a directory
  private def addDirToZipArchive(zos: ZipOutputStream, fileToZip: File, parentDirectoryName: Option[String] = None): Unit = {
    if (fileToZip != null || fileToZip.exists) {
      val zipEntryName = parentDirectoryName match {
        case Some(name) => s"$name/${fileToZip.getName}"
        case _ => fileToZip.getName
      }
      fileToZip.isDirectory match {
        case true => // Process directory
          fileToZip.listFiles.foreach(addDirToZipArchive(zos, _, Some(zipEntryName)))
        case _ => //individual file
          zos.putNextEntry(new ZipEntry(zipEntryName))
          val fis = new FileInputStream(fileToZip)
          val buffer = new Array[Byte](4096)
          Stream.continually(fis.read(buffer)).takeWhile(_ != -1).foreach(zos.write(buffer, 0, _))
          zos.closeEntry()
          fis.close()
      }
    }
  }

  // Get a byte array containing all the files in the directory
  def getZippedMessage(source: File) : Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(bos)
    addDirToZipArchive(zos, source, None)
    bos.toByteArray
  }
}
