package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ ModelBase, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import org.tensorflow.{ Graph, Session }

/**
 * Abstract class for any TensorFlow (optimized export) model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowModel[RECORD, SCORE, RESULT](descriptor: ModelDescriptor)
  extends ModelBase[RECORD, SCORE, RESULT](descriptor) with Serializable {

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
}
