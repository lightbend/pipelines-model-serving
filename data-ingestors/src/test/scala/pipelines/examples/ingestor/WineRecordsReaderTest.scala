package pipelines.examples.ingestor

import org.scalatest.{ FunSpec, BeforeAndAfter }
import java.io.ByteArrayOutputStream

class WineRecordsReaderTest extends FunSpec with BeforeAndAfter with OutputInterceptor {

  val initializingMsgFmt = "WineRecordsReader: Initializing from resource %s"
  val testGoodRecordsResources = Array("100_winequality_red.csv")
  val testBadRecordsResources = Array("error_winequality_red.csv")

  describe("WineRecordsReader") {
    it("Loads one or more CSV file resources from the classpath") {
      ignoreOutput {
        assert(WineRecordsReader(WineDataIngress.WineQualityRecordsResources).next() != null)
      }
    }
    it("Raises an exception if the resource doesn't exist") {
      ignoreOutput {
        intercept[IllegalArgumentException] {
          WineRecordsReader(Seq("foobar"))
        }
      }
    }
    it("Asserts if the input sequence of resources is empty") {
      ignoreOutput {
        intercept[AssertionError] {
          WineRecordsReader(Nil)
        }
      }
    }
    describe("next") {
      it("Continuously rereads the resource until terminated") {
        val outMsgs = Array.fill(2)(initializingMsgFmt.format(testGoodRecordsResources(0)))
        expectOutput(outMsgs) {
          val reader = WineRecordsReader(testGoodRecordsResources)
          (0 until 101).foreach(_ ⇒ reader.next())
        }
      }
      it("Prints errors for bad records") {
        val outMsgs = testBadRecordsResources.map(s ⇒ initializingMsgFmt.format(s))
        // A bit fragile hard-coding all these strings, but they exactly match the "bad" input file.
        val errMsgs = Array(
          "ERROR: record does not have 11 fields after splitting string on ';': 7.4;0.7;0;1.9;0.076;11;34;0.9978;3.51;0.56",
          "ERROR: Failed to parse string ;0.7;0;1.9;0.076;11;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;;0;1.9;0.076;11;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;;1.9;0.076;11;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;;0.076;11;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;;11;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;;34;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;11;;0.9978;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;11;34;;3.51;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;11;34;0.9978;;0.56;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;11;34;0.9978;3.51;;9.4;5. cause: java.lang.NumberFormatException: empty String",
          "ERROR: Failed to parse string 7.4;0.7;0;1.9;0.076;11;34;0.9978;3.51;0.56;;5. cause: java.lang.NumberFormatException: empty String")

        expectOutput(outMsgs, errMsgs) {
          val reader = WineRecordsReader(testBadRecordsResources)
          // Note that the first record is good, so it will be read successfully.
          // It will be the second call to next() that will return 12 error messages!
          (0 until 2).foreach(_ ⇒ reader.next())
        }
      }
    }
  }
}
