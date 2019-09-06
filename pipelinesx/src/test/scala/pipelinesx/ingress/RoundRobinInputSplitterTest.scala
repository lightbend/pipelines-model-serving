package pipelinesx.ingress

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import org.scalatest.FlatSpec

class RoundRobinInputSplitterTest extends FlatSpec {

  implicit val system: ActorSystem = ActorSystem("ModelServing")
  implicit val executor = system.getDispatcher
  implicit val materializer = ActorMaterializer()

  "Generation of input" should "create round robin output" in {

    val sink1 = Sink.foreach[Int](value ⇒ println(s"sink1 - value $value"))
    val sink2 = Sink.foreach[Int](value ⇒ println(s"sink2 - value $value"))
    val sink3 = Sink.foreach[Int](value ⇒ println(s"sink3 - value $value"))
    val sink4 = Sink.foreach[Int](value ⇒ println(s"sink4 - value $value"))

    new RoundRobinInputSplitter[Int](sink1, sink2, sink3, sink4) {
      def source = Source(1 to 16)
    }.runnableGraph().run()
  }
}
