package pipelines.examples.modelserving

import pipelines.akkastream.scaladsl.Merge

import pipelines.examples.data._
import pipelines.examples.data.Codecs._

object MergeMLActions extends Merge[MlAction](2)
