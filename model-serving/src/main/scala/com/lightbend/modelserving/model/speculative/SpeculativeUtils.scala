package com.lightbend.modelserving.model.speculative

import java.io.{ DataInputStream, DataOutputStream }
import com.lightbend.modelserving.speculative.SpeculativeStreamMerger

object SpeculativeUtils {

  /**
   * Write speculative configuration to a stream.
   */
  def write(merger: SpeculativeStreamMerger, output: DataOutputStream): Unit = {
    output.writeLong(merger.timeout)
    output.writeLong(merger.results.toLong)
  }

  /**
   * Read speculative configuration from a stream.
   */
  def read(input: DataInputStream): SpeculativeStreamMerger = {
    val tmout = input.readLong()
    val results = input.readLong().toInt
    new SpeculativeStreamMerger(tmout, results)
  }
}
