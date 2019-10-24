package pipelinesx.queue

import akka.actor.ActorSystem
import akka.stream.{ ActorMaterializer, OverflowStrategy }
import akka.stream.scaladsl.{ Source, Sink }

// Simple test for a queue test
object QueueTest {

  implicit val system = ActorSystem("ModelServing")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def main(args: Array[String]): Unit = {

    val bufferSize = 10

    val queue = Source.queue[Int](bufferSize, OverflowStrategy.fail)
      .to(Sink.foreach(v ⇒ println(s"getting $v from the queue")))
      .run

    for (i ← 1 to 10) {
      Thread.sleep((600 * i).toLong)
      println(s"Putting $i to the queue")
      queue.offer(i)
    }
    Thread.sleep(60)
  }
}
