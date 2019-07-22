package com.lightbend.modelserving.model.tensorflow

import java.io.{ ObjectInputStream, ObjectOutputStream }

import com.lightbend.modelserving.model.{ Model, ModelMetadata }
import org.tensorflow.{ Graph, Session }
import com.lightbend.modelserving.model.ModelType

/**
 * Abstract class for any TensorFlow (optimized export) model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowModel[RECORD, RESULT](val metadata: ModelMetadata)
  extends Model[RECORD, RESULT] with Serializable {

  private def setup(): (Graph, Session) = {
    // Make sure data is not empty
    if (metadata.modelBytes.length == 0) throw new RuntimeException("Empty graph data")
    // Model graph
    val graph = new Graph
    graph.importGraphDef(metadata.modelBytes)
    // Create TensorFlow session
    val session = new Session(graph)
    (graph, session)
  }
  val (graph, session) = setup()

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

  // TODO: Verify if these methods are actually needed, since they have only one field,
  // the metadata, which has these methods:
  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeObject(metadata)
  //   println(s"TensorFlow optimized serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   metadata = input.readObject().asInstanceOf[ModelMetadata]
  //   try {
  //     val (graph, session) = setup()
  //     println(s"TensorFlow model deserialization in ${System.currentTimeMillis() - start} ms")
  //   } catch {
  //     case t: Throwable ⇒
  //       throw new RuntimeException(
  //         s"TensorFlow model deserialization failed in ${System.currentTimeMillis() - start} ms", t)
  //   }
  // }
}
