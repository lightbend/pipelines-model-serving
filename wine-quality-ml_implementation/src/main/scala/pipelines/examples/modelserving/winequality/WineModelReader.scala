package pipelines.examples.modelserving.winequality

import java.io.ByteArrayOutputStream

import com.lightbend.modelserving.model.{ ModelDescriptor, ModelType }

/**
 * Provides an infinite stream of wine records, repeatedly reading them from
 * the specified resource.
 */
final case class WineModelReader(resourceNames: Map[ModelType, Seq[String]]) {

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
        modelType = ModelType.TENSORFLOW,
        modelName = s"Tensorflow Model - $resourceName",
        description = "generated from TensorFlow",
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
        modelType = ModelType.PMML,
        modelName = resourceName.dropRight(5),
        description = "generated from Spark",
        modelBytes = Some(barray),
        modelSourceLocation = None)

    case ModelType.TENSORFLOWSERVING | ModelType.TENSORFLOWSAVED ⇒
      Console.err.println(
        s"BUG! currentModelType = $currentModelType should not be set! Using TENSORFLOW")
      init(ModelType.TENSORFLOW)
      next()
  }

  protected def readBytes(source: String): Array[Byte] = {
    val is = this.getClass.getClassLoader.getResourceAsStream(source)
    val buffer = new Array[Byte](1024)
    val content = new ByteArrayOutputStream()
    Stream.continually(is.read(buffer)).takeWhile(_ != -1).foreach(content.write(buffer, 0, _))
    content.toByteArray
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
