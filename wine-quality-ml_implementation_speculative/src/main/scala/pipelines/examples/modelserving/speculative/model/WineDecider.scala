package pipelines.examples.modelserving.speculative.model

import com.lightbend.modelserving.model.ModelResultMetadata
import com.lightbend.modelserving.model.speculative.Decider
import pipelines.examples.modelserving.winequality.data.{ WineRecord, WineResult }
import pipelines.examples.modelserving.winequality.result.ModelDoubleResult
import util.control.Breaks._

class WineDecider extends Decider[WineResult] {

  private val random = new scala.util.Random

  // The simplest decider returning the last result
  override def decideResult(results: List[WineResult]): WineResult = {

    results.size match {
      case 0 ⇒ // No results, can only happen if we timed out
        WineResult(
          WineRecord("", .0, .0, .0, .0, .0, .0, .0, .0, .0, .0, .0),
          ModelDoubleResult(0.0),
          ModelResultMetadata("All of the models timed out", "UNKNOWN", "", 0L, 0L))
      case _ ⇒ // We have results
        var result = results.head // Start with the first one
        breakable {
          results.foreach(res ⇒ {
            if (res.modelResultMetadata.errors.isEmpty) {
              result = res
              if (random.nextFloat() > .5)
                break
            }
          })
        }
        result
    }
  }
}
