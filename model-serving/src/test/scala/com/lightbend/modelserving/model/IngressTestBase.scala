package com.lightbend.modelserving.model

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Sink, Source }
import org.scalatest.FlatSpec

import scala.concurrent.duration._

import com.lightbend.modelserving.model.ModelDescriptorUtil.implicits._

/**
 * Implements the boilerplate required for all data read testing classes in the
 * pipelines example apps.
 *
 * @param count the default value for the number of records or models to process.
 * @param frequencyMillis the default value for _milliseconds_ between each one.
 *        Note that model classes may define their default values in seconds, so convert!
 */
abstract class IngressTestBase[T](
    val count:           Int            = 10,
    val frequencyMillis: FiniteDuration = 1000.milliseconds) extends FlatSpec {

  /**
   * Make a Source of instances. Implement this for your record type "T" for record
   * tests, use [[ModelDescriptor]] for "T" for model tests.
   * @param frequency if your Source already provides throttling. Otherwise, ignore.
   */
  protected def makeSource(frequency: FiniteDuration): Source[T, NotUsed]

  /**
   * Normally, this default implementation or the implementation in
   * the ModelMainBase will be sufficient, but override it when needed.
   */
  protected def toString(t: T): String = t.toString

  // Akka restricts names for actors to [a-zA-Z0-9_-], where '_' and '-' can't
  // be the first character.
  val className = this.getClass.getName.replaceAll("[^a-zA-Z0-9_-]", "_")

  def readData(): Unit = {
    implicit val system = ActorSystem(className)
    implicit val mat = ActorMaterializer()
    var i = 1
    makeSource(frequencyMillis).runWith {
      Sink.foreach { t ⇒
        println(toString(t))
        if (i == count) {
          println("Finished!")
          sys.exit(0)
        }
        i += 1
      }
    }
    ()
  }
}

abstract class ModelIngressTestBase(
    count:           Int            = 10,
    frequencyMillis: FiniteDuration = 1000.milliseconds)
  extends IngressTestBase[ModelDescriptor](count, frequencyMillis) {

  override protected def toString(m: ModelDescriptor): String = m.toRichString
}
