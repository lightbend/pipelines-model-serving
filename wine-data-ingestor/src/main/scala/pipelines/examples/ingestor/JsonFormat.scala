package pipelines.examples.ingestor

import pipelines.examples.data.ModelDescriptor
import spray.json._

object ModelDescriptorJsonSupport extends DefaultJsonProtocol {
  implicit val modelDescriptorFormat = jsonFormat(ModelDescriptor.apply, "name", "description",
    "dataType", "modeltype", "data", "location")
}
