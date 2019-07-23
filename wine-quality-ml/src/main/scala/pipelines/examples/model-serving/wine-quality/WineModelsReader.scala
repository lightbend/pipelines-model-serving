package pipelines.examples.modelserving.winequality

import pipelinesx.ingress.ByteArrayReader
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }

/**
 * Provides an infinite stream of wine records, repeatedly reading them from
 * the specified resource.
 */
final case class WineModelsReader(resourceNames: Map[ModelType, Seq[String]]) {

  assert(resourceNames.size > 0)

  protected var currentModelType: ModelType = ModelType.TENSORFLOW
  protected var currentIndex = 0
  init(ModelType.TENSORFLOW)

  // ModelTypes defined in the Avro files: ["TENSORFLOW", "TENSORFLOWSAVED", "TENSORFLOWSERVING", "PMML"]
  def next(): ModelDescriptor = currentModelType match {
    case ModelType.TENSORFLOW if finished(ModelType.TENSORFLOW, currentIndex) ⇒
      init(ModelType.PMML)
      next()

    case ModelType.TENSORFLOW ⇒
      val resourceName = resourceNames(ModelType.TENSORFLOW)(currentIndex)
      val barray = readBytes(resourceName)
      currentIndex += 1
      new ModelDescriptor(
        name = s"Tensorflow Model - $resourceName",
        description = "generated from TensorFlow",
        dataType = "wine",
        modelType = ModelType.TENSORFLOW,
        modelBytes = Some(barray),
        modelSourceLocation = None)

    case ModelType.PMML if finished(ModelType.PMML, currentIndex) ⇒
      init(ModelType.TENSORFLOW)
      next()

    case ModelType.PMML ⇒
      val resourceName = resourceNames(ModelType.PMML)(currentIndex)
      val barray = readBytes(resourceName)
      currentIndex += 1
      new ModelDescriptor(
        name = resourceName.dropRight(5),
        description = "generated from Spark",
        dataType = "wine",
        modelType = ModelType.PMML,
        modelBytes = Some(barray),
        modelSourceLocation = None)

    case ModelType.TENSORFLOWSERVING | ModelType.TENSORFLOWSAVED ⇒
      Console.err.println(
        s"BUG! currentModelType = $currentModelType should not be set! Using TENSORFLOW")
      init(ModelType.TENSORFLOW)
      next()
  }

  protected def readBytes(source: String): Array[Byte] =
    ByteArrayReader.fromClasspath(source) match {
      case Left(error) ⇒
        throw new IllegalArgumentException(error) // TODO: return Either from readBytes!
      case Right(array) ⇒ array
    }

  protected def finished(modelType: ModelType, currentIndex: Int): Boolean =
    resourceNames.get(modelType) match {
      case None                                      ⇒ true
      case Some(names) if currentIndex >= names.size ⇒ true
      case _                                         ⇒ false
    }

  protected def init(whichType: ModelType): Unit = {
    currentModelType = whichType
    currentIndex = 0
    if (finished(whichType, 0))
      println(s"WARNING: No resources specified for model type $whichType")
  }
}

object WineModelsReader {

  /** For testing purposes. */
  def main(args: Array[String]): Unit = {
    val count = if (args.length > 0) args(0).toInt else 100000

    val reader = new WineModelsReader(WineModelDataIngressUtil.wineModelsResources)
    (1 to count).foreach { n ⇒
      val model = reader.next()
      println("%7d: %s".format(n, model))
    }
  }
}
