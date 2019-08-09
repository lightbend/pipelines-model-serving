package com.lightbend.modelserving.model.splitter

import com.lightbend.modelserving.splitter.{ OutputPercentage, StreamSplitter }

import scala.collection.mutable.ListBuffer

object StreamSplitterUtil {

  def splitdefinition(splitter: StreamSplitter): (Array[Int], Array[Int]) = {
    val seq = splitter.split.map(v ⇒ (v.output, v.percentage)).unzip
    (seq._1.toArray, seq._2.toArray)
  }

  def main(args: Array[String]): Unit = {

    val inputs = new ListBuffer[OutputPercentage]()
    0 to 2 foreach (i ⇒ {
      val output = i
      val percentage = 33
      inputs += new OutputPercentage(output, percentage)
    })
    val split = new StreamSplitter(inputs)
    val definition = splitdefinition(split)
    val inps = definition._1
    val probs = definition._2
    println(s"inps - $inps; probs - $probs")
  }
}
