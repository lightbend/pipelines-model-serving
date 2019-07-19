package com.lightbend.modelserving.model.tensorflow

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.lightbend.modelserving.model.Model
import org.tensorflow.{ Graph, Session }
import com.lightbend.modelserving.model.ModelType

/**
 * Abstract class for any TensorFlow (optimized export) model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowModel[RECORD, RESULT](inputStream: Array[Byte]) extends Model[RECORD, RESULT] with Serializable {

  // Make sure data is not empty
  if (inputStream.length < 1) throw new Exception("Empty graph data")
  // Model graph
  var graph = new Graph
  graph.importGraphDef(inputStream)
  // Create TensorFlow session
  var session = new Session(graph)
  var bytes = inputStream

  override def cleanup(): Unit = {
    try {
      session.close
    } catch {
      case _: Throwable ⇒ // Swallow
    }
    try {
      graph.close
    } catch {
      case _: Throwable ⇒ // Swallow
    }
  }

  /** Convert the TensorFlow model to bytes */
  override def toBytes(): Array[Byte] = bytes

  /** Get model type */
  override def getType = ModelType.TENSORFLOW

  override def equals(obj: Any): Boolean = {
    obj match {
      case tfModel: TensorFlowModel[RECORD, RESULT] ⇒
        tfModel.toBytes.toList == inputStream.toList
      case _ ⇒ false
    }
  }

  private def writeObject(output: ObjectOutputStream): Unit = {
    val start = System.currentTimeMillis()
    output.writeObject(bytes)
    println(s"TensorFlow optimized serialization in ${System.currentTimeMillis() - start} ms")
  }

  private def readObject(input: ObjectInputStream): Unit = {
    val start = System.currentTimeMillis()
    bytes = input.readObject().asInstanceOf[Array[Byte]]
    try {
      graph = new Graph
      graph.importGraphDef(bytes)
      session = new Session(graph)
      println(s"TensorFlow optimized deserialization in ${System.currentTimeMillis() - start} ms")
    } catch {
      case t: Throwable ⇒
        t.printStackTrace
        println(s"TensorFlow optimized deserialization failed in ${System.currentTimeMillis() - start} ms")
    }
  }
}
