package pipelines.examples.modelserving.airlineflights.models

import pipelines.examples.modelserving.airlineflights.data.{ AirlineFlightRecord, AirlineFlightResult}
import com.lightbend.modelserving.model.{ ModelFactory, ModelFactoryResolver }

/**
 * Model factory resolver - requires specific factories
 */
object AirlineFlightFactoryResolver extends ModelFactoryResolver[AirlineFlightRecord, AirlineFlightResult] {

  private val factories = Map(
    ModelType.H2O.ordinal -> AirlineFlightH2OModel)

  override def getFactory(whichFactory: Int): Option[ModelFactory[AirlineFlightRecord, AirlineFlightResult]] = factories.get(whichFactory)
}
