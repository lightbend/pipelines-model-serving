package pipelines.examples.modelserving.winequality

import com.lightbend.modelserving.model.persistence.FilePersistence
import pipelines.examples.modelserving.winequality.data.WineResult
import pipelines.akkastream.AkkaStreamlet
import pipelines.streamlets.{ ReadWriteMany, StreamletShape, VolumeMount }
import pipelines.streamlets.avro.AvroInlet
import pipelinesx.egress.DiskEgressLogic

final case object WineResultDiskEgress extends AkkaStreamlet {
  val in = AvroInlet[WineResult]("in")
  final override val shape = StreamletShape.withInlets(in)

  // Declare the volume mount: 
  private val mountPoint = "/log"
  private val persistentDataMount =
    VolumeMount("log-data-mount", mountPoint, ReadWriteMany)
  override def volumeMounts = Vector(persistentDataMount)
  FilePersistence.setGlobalMountPoint(persistentDataMount.path)

  override def createLogic = DiskEgressLogic[WineResult](
    in = in,
    outdir = mountPoint
  )
}
