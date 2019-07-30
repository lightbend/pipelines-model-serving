package com.lightbend.modelserving.model.tensorflow

import com.lightbend.modelserving.model.{ ModelBase, ModelDescriptor }
import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

import org.tensorflow.{ Graph, Session }

/**
 * Abstract class for any TensorFlow (optimized export) model processing. It has to be extended by the user
 * implement score method, based on his own model. Serializability here is required for Spark.
 */
abstract class TensorFlowModel[RECORD, MODEL_OUTPUT](descriptor: ModelDescriptor)(makeDefaultModelOutput: () ⇒ MODEL_OUTPUT)
  extends ModelBase[RECORD, MODEL_OUTPUT](descriptor)(makeDefaultModelOutput) with Serializable {

  assert(descriptor.modelBytes != None, s"Invalid descriptor ${descriptor.toRichString}")

  type Signatures = Map[String, Signature]

  // setup is different for each TensorFlow model kind, so we hide these differences
  // behind a protected method init below.
  private def setup(): (Graph, Session, Signatures) = {
    // Model graph
    val graph = new Graph
    graph.importGraphDef(descriptor.modelBytes.get)
    // Create TensorFlow session
    val session = new Session(graph)
    (graph, session, Map.empty)
  }

  protected def init(): (Graph, Session, Signatures) = setup()

  val (graph, session, _) = init()

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
