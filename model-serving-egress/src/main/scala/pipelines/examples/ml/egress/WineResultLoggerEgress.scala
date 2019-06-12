package pipelines.examples.ml.egress

import pipelines.examples.data.DataCodecs._
import pipelines.examples.data._

object WineResultLoggerEgress extends PrintlnLoggerEgress[WineResult] {
  val prefix: String = "Wine Quality: "
}
