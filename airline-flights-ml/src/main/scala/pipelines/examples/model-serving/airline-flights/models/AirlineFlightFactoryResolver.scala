package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult }
import com.lightbend.modelserving.model.{ ModelDescriptor, ModelFactory, ModelFactoryResolver, ModelType }

/**
 * Model factory resolver - requires specific factories
 */
object AirlineFlightFactoryResolver extends ModelFactoryResolver[AirlineFlightRecord, AirlineFlightResult] {

  private val factories = Map(
    ModelType.H2O -> AirlineFlightH2OModel)

  override def getFactory(descriptor: ModelDescriptor): Option[ModelFactory[AirlineFlightRecord, AirlineFlightResult]] =
    factories.get(descriptor.modelType)
}
