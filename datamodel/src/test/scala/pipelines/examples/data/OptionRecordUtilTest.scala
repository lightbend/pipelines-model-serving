//package pipelines.examples.data
//
//import org.scalatest.FunSpec
//
//class OptionRecordUtilTest extends FunSpec {
//
//  val good = "^VIX,2016-02-10 09:31:00,VIX,2016-02-10,10.000,c,0.00,0.00,0.00,0.00,0,0,0.00,0,0.00,0.00,0.00,0.00,0.00,0.0000,0.0000,0.000000,0.000000,0.000000,0.000000"
//  val goodRecord = OptionRecord(
//    "^VIX", "2016-02-10 09:31:00", "VIX", "2016-02-10", 10.000, "c",
//    0.00, 0.00, 0.00, 0.00, 0, 0,
//    0.00, 0, 0.00, 0.00, 0.00, 0.00,
//    0.00, 0.0000, 0.0000, 0.000000, 0.000000, 0.000000, 0.000000, false)
//
//  describe("OptionRecordUtil") {
//    describe("toOptionRecord") {
//      it("A CSV string that doesn't have 25 fields returns an error (Left)") {
//        Seq(0, 1, 24, 26) foreach { n =>
//          OptionRecordUtil.toOptionRecord(Array.fill[String](n)("")) match {
//            case Left(msg) => assert(msg.contains("does not have 25 fields"), s"n: $n, msg: $msg")
//            case _ =>
//          }
//        }
//      }
//
//      it("A CSV string with one or more fields of the wrong type returns an error (Left)") {
//        OptionRecordUtil.toOptionRecord(Array.fill[String](25)("")) match {
//          case Left(msg) => assert(msg.contains("Failed to convert CSV to OptionRecord"), msg)
//          case _ =>
//        }
//        4 +: (6 until 25) foreach { index =>
//          val array = good.split(",")
//          array(index) = "bad"
//          OptionRecordUtil.toOptionRecord(array) match {
//            case Left(msg) => assert(msg.contains("Failed to convert CSV to OptionRecord"), msg)
//            case _ =>
//          }
//        }
//      }
//
//      it("A CSV string with valid fields returns a valid Right[OptionRecord]") {
//        OptionRecordUtil.toOptionRecord(good.split(",")) match {
//          case Right(or) => assert(goodRecord === or)
//          case Left(msg) => fail(msg)
//        }
//      }
//    }
//  }
//}
