package pipelines.examples.modelserving.winequality

import org.scalatest.{ FunSpec, BeforeAndAfterAll }
import com.lightbend.modelserving.model.ModelType
import pipelinesx.test.OutputInterceptor

class WineModelsReaderTest extends FunSpec with BeforeAndAfterAll with OutputInterceptor {

  override def afterAll: Unit = {
    resetOutputs()
  }

  val initializingMsgFmt = "WineModelsReader: Initializing from resource %s"
  val testGoodModelsResources = Array("wine/data/100_winequality_red.csv")
  val testBadModelsResources = Array("wine/data/error_winequality_red.csv")

  describe("WineModelsReader") {
    it("Loads one or more model file resources from the classpath") {
      ignoreOutput {
        assert(WineModelsReader(WineModelDataIngressUtil.wineModelsResources).next() != null)
      }
    }
    it("Asserts if the input map of resources is empty") {
      ignoreOutput {
        intercept[AssertionError] {
          WineModelsReader(Map.empty)
        }
      }
    }
    it("Accepts an empty list of resources for a model type") {
      ignoreOutput {
        WineModelsReader(Map(ModelType.TENSORFLOW -> Nil))
      }
    }
    it("Warns if an empty list of resources is specified for a model type") {
      expectOutput(Array("WARNING: No resources specified for model type TENSORFLOW")) {
        WineModelsReader(Map(ModelType.TENSORFLOW -> Nil))
      }
    }

    describe("next") {
      it("Raises an exception if the resource doesn't exist") {
        ignoreOutput {
          intercept[IllegalArgumentException] {
            WineModelsReader(Map(ModelType.PMML -> Seq("foobar"))).next()
          }
        }
      }

      it("Continuously rereads the resource until terminated") {
        // val outMsgs = Array.fill(2)(initializingMsgFmt.format(testGoodModelsResources(0)))
        ignoreOutput {
          val reader = WineModelsReader(WineModelDataIngressUtil.wineModelsResources)
          val totalPMML = WineModelDataIngressUtil.wineModelsResources(ModelType.PMML).size * 2
          val totalTensorFlow = WineModelDataIngressUtil.wineModelsResources(ModelType.TENSORFLOW).size * 2

          val totalN = totalPMML + totalTensorFlow
          val (countPMML, countTensorFlow) = (0 until totalN).foldLeft((0, 0)) {
            case ((countPMML, countTensorFlow), _) ⇒
              val modelDescriptor = reader.next()
              modelDescriptor.modelType match {
                case ModelType.PMML       ⇒ (countPMML + 1, countTensorFlow)
                case ModelType.TENSORFLOW ⇒ (countPMML, countTensorFlow + 1)
                case other                ⇒ fail(s"Bad map key: $other")
              }
          }
          assert(totalPMML == countPMML)
          assert(totalTensorFlow == countTensorFlow)
        }
      }
    }
  }
}
