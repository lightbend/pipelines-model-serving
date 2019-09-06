package com.lightbend.modelserving.model.splitter

import java.io.{ DataInputStream, DataOutputStream }
import com.lightbend.modelserving.splitter.{ OutputPercentage, StreamSplitter }
import scala.collection.mutable.ListBuffer

object StreamSplitterUtil {

  def splitdefinition(splitter: StreamSplitter): (Array[Int], Array[Int]) = {
    val seq = splitter.split.map(v ⇒ (v.output, v.percentage)).unzip
    (seq._1.toArray, seq._2.toArray)
  }

  /**
   * Write speculative configuration to a stream.
   */
  def write(splitter: StreamSplitter, output: DataOutputStream): Unit = {
    output.writeLong(splitter.split.size.toLong)
    splitter.split.foreach(record ⇒ {
      output.writeLong(record.output.toLong)
      output.writeLong(record.percentage.toLong)
    })
  }

  /**
   * Read speculative configuration from a stream.
   */
  def read(input: DataInputStream): StreamSplitter = {
    val ninputs = input.readLong().toInt
    val inputs = new ListBuffer[OutputPercentage]()
    0 to ninputs - 1 foreach (i ⇒ {
      val output = input.readLong().toInt
      val percentage = input.readLong().toInt
      inputs += new OutputPercentage(output, percentage)
    })
    new StreamSplitter(inputs)
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
