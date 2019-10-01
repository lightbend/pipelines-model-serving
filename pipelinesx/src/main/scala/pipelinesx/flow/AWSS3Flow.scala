package pipelinesx.flow

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.alpakka.s3.MultipartUploadResult
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.{Flow, Source}
import pipelines.akkastream.PipelinesContext

import scala.concurrent.ExecutionContext

/**
 * Flow for writing set of messages to S3 and propagate result (MultipartUploadResult) with Pipeline context
 *
 * @param bucket AWS S3 bucket
 * @param keyPrefix AWS S3 key prefix
 * @param transformer a function transforming input message to String - defaults to [[t.toString + "/n"]].
 * @param maxSize max size for collected data
 * @param duration max duration to collect
 */
final case class AWSS3Flow[T](
    bucket:    String,
    keyPrefix: String,
    transformer:  (T) => String = (t: T) => t.toString + "/n",
    maxSize:      Long = 512000000,
    duration:     Long = 3600000)
    (implicit val materializer : Materializer,
     implicit val executionContext: ExecutionContext) {

  val collector = new DataCollector[T](transformer, maxSize, duration)

  def S3Flow: Flow[(T, PipelinesContext), (MultipartUploadResult, PipelinesContext), NotUsed] =
    collector.collectorFlow.mapAsync(1) { p =>
      val fileName = p._1._1
      val data = p._1._2
      val context = p._2
      Source.single(data)
        .runWith(S3.multipartUpload(bucket, s"$keyPrefix/$fileName"))
        .map(r => (r, context))
    }
}