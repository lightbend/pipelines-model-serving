package pipelinesx.flow

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.s3.scaladsl.S3
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.scalatest.FlatSpec
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.Await
import scala.concurrent.duration._

// Testing for S3 upload
class S3Test extends FlatSpec with ScalaFutures {

  implicit val actorSystem: ActorSystem = ActorSystem("S3IntegrationSpec")
  implicit val materializer = ActorMaterializer()
  implicit val ec = materializer.executionContext

  it should "upload multipart with real credentials" in {
    val source: Source[ByteString, Any] = Source(ByteString("Some very complex string") :: Nil)

    val result =
      source
        .runWith(
          S3.multipartUpload("fdp-killrweather-data", "edrusco/test")
        )

    val multipartUploadResult = Await.ready(result, 90.seconds).futureValue
    println(s"Uploaded file to bucket ${multipartUploadResult.bucket}, key ${multipartUploadResult.key}, etag ${multipartUploadResult.etag}")
  }
}
