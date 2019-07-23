package pipelines.examples.ingestor

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.OutputMode
import pipelines.examples.data.WineRecord
import pipelines.spark.sql.SQLImplicits._
import pipelines.spark.{ SparkStreamlet, SparkStreamletLogic }
import pipelines.streamlets.StreamletShape
import pipelines.streamlets.avro.{ AvroInlet, AvroOutlet }

class WineSparkAgg extends SparkStreamlet {
  val in = AvroInlet[WineRecord]("in")
  val out = AvroOutlet[WineRecord]("out", _.dataType.toString)
  val shape = StreamletShape(in, out)

  override def createLogic() = new SparkStreamletLogic {
    override def buildStreamingQueries = {
      val dataset = readStream(in)
      val outStream = process(dataset)
      writeStream(outStream, out, OutputMode.Update).toQueryExecution
    }

    private def process(inDataset: Dataset[WineRecord]): Dataset[WineRecord] = {
      inDataset
    }
  }
}
