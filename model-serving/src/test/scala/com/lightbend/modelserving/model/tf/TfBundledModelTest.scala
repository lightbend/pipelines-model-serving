package com.lightbend.modelserving.model.tf

import java.io.{ ByteArrayInputStream, ByteArrayOutputStream, File, FileInputStream, FileOutputStream }
import java.util.zip.{ ZipEntry, ZipInputStream, ZipOutputStream }

import org.scalatest.FlatSpec

class TfBundledModelTest extends FlatSpec {

  val modelPath = "model-serving/src/test/resources/tfsavedmodel/savedmodel.zip"
  val sourceModelPath = "model-serving/src/test/resources/saved"
  val workingDirectory = "tmp"

  def deleteRecursively(file: File): Unit = {
    if (file.exists) {
      if (file.isDirectory) {
        file.listFiles.foreach(deleteRecursively)
      }
      val _ = file.delete
    }
  }

  def deleteDirectoryContent(file: File): Unit = {
    if (file.exists) {
      if (file.isDirectory)
        file.listFiles.foreach(deleteRecursively)
    }
  }

  def unzipMessage(data: Array[Byte], directory: String): String = {
    val destination = new File(directory)
    val zis = new ZipInputStream(new ByteArrayInputStream(data))
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).filter(!_.isDirectory).foreach(entry ⇒ {
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

  def addDirToZipArchive(zos: ZipOutputStream, fileToZip: File, parentDirectoryName: Option[String] = None): Unit = {
    if (fileToZip != null || fileToZip.exists) {
      val zipEntryName = parentDirectoryName match {
        case Some(name) ⇒ s"$name/${fileToZip.getName}"
        case _          ⇒ fileToZip.getName
      }
      fileToZip.isDirectory match {
        case true ⇒ // Process directory
          //          println(s"processing directory $zipEntryName")
          fileToZip.listFiles.foreach(addDirToZipArchive(zos, _, Some(zipEntryName)))
        case _ ⇒ //individual file
          //          println(s"processing file $zipEntryName")
          zos.putNextEntry(new ZipEntry(zipEntryName))
          val fis = new FileInputStream(fileToZip)
          val buffer = new Array[Byte](4096)
          Stream.continually(fis.read(buffer)).takeWhile(_ != -1).foreach(zos.write(buffer, 0, _))
          zos.closeEntry()
          fis.close()
      }
    }
  }

  val directoryFile = new File(workingDirectory)
  // Start by deleting the existing content
  deleteDirectoryContent(directoryFile)

  "Source directory" should "create a zip file" in {
    val source = new File(sourceModelPath)
    val bos = new ByteArrayOutputStream()
    val zos = new ZipOutputStream(bos)
    addDirToZipArchive(zos, source, None)
    val fis = new FileOutputStream(s"$workingDirectory/message.zip")
    fis.write(bos.toByteArray)
    fis.close()
  }

  "Incomming messages" should "be writtent to file" in {
    val fis = new FileInputStream(s"$workingDirectory/message.zip")
    val available = fis.available
    val buffer = Array.fill[Byte](available)(0)
    val _ = fis.read(buffer)
    println(unzipMessage(buffer, "tmp"))
  }
}
