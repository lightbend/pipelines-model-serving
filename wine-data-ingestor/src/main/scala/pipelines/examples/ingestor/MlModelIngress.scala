package pipelines.examples.ingestor

import akka.http.scaladsl.common.EntityStreamingSupport
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import pipelines.akkastream.scaladsl._
import pipelines.examples.data._
import pipelines.examples.data.Codecs._

import ModelDescriptorJsonSupport._

object MlModelIngress extends HttpIngress[ModelDescriptor]
