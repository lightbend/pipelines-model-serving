package pipelines.examples.modelserving.airlineflightmodel

import com.lightbend.modelserving.model.{ ModelFactory, ModelFactoryResolver }
import pipelines.examples.data.{ AirlineFlightRecord, AirlineFlightResult, ModelType }

/**
 * Model factory resolver - requires specific factories
 */
object AirlineFlightFactoryResolver extends ModelFactoryResolver[AirlineFlightRecord, AirlineFlightResult] {

  private val factories = Map(
    ModelType.H2O.ordinal -> AirlineFlightH2OModel)

  override def getFactory(whichFactory: Int): Option[ModelFactory[AirlineFlightRecord, AirlineFlightResult]] = factories.get(whichFactory)
}
