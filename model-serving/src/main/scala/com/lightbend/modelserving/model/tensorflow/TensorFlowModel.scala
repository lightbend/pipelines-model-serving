package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ Model, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import org.tensorflow.{ Graph, Session }

/**
 * Abstract class for any TensorFlow (optimized export) model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowModel[RECORD, RESULT](val descriptor: ModelDescriptor)
  extends Model[RECORD, RESULT] with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  private def setup(): (Graph, Session) = {
    // Model graph
    val graph = new Graph
    graph.importGraphDef(descriptor.modelBytes.get)
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
  // the descriptor, which has these methods:
  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeObject(descriptor)
  //   println(s"TensorFlow optimized serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   descriptor = input.readObject().asInstanceOf[ModelDescriptor]
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
