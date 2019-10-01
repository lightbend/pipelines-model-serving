package pipelinesx.egress

import pipelines.akkastream._
import pipelines.akkastream.scaladsl.RunnableGraphStreamletLogic
import pipelines.streamlets._
import pipelinesx.flow.AWSS3Flow

/**
 * Abstraction for writing to output to AWS S3.
 * @param in            CodecInlet for records of type IN
 * @param bucket:       String,
 * @param keyPrefix:    String,
 * @param transformer:  (IN) => String = (in: IN) => in.toString + "/n",
 * @param maxSize:      Long = 512000000,
 * @param duration:     Long = 3600000)
 */
final case class AWSS3EgressLogic[IN](
    in:           CodecInlet[IN],
    bucket:       String,
    keyPrefix:    String,
    transformer:  (IN) => String = (t: IN) => t.toString + "/n",
    maxSize:      Long = 512000000,
    duration:     Long = 3600000)
    (implicit val context: StreamletContext)
  extends RunnableGraphStreamletLogic {

  var AWSS3 = new AWSS3Flow[IN](bucket, keyPrefix)

  override def runnableGraph() = atLeastOnceSource(in)
    .via(AWSS3.S3Flow).map(result => println(s""))
    .to(atLeastOnceSink)
}