package com.lightbend.modelserving.model.speculative

/**
 * An interface to be implemented by implementation that chooses result from multiple model servers
 */

trait Decider[Result] {

  def decideResult(results: List[Result]): Result
}
