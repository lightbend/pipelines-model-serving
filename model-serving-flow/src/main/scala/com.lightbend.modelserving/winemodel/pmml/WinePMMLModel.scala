/*
 * Copyright (C) 2017-2019  Lightbend
 *
 * This file is part of the Lightbend model-serving-tutorial (https://github.com/lightbend/model-serving-tutorial)
 *
 * The model-serving-tutorial is free software: you can redistribute it and/or modify
 * it under the terms of the Apache License Version 2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.lightbend.modelserving.winemodel.pmml

import com.lightbend.modelserving.model.{ Model, ModelFactory }
import com.lightbend.modelserving.model.ModelToServe
import com.lightbend.modelserving.model.pmml.PMMLModel
import org.jpmml.evaluator.Computable
import pipelines.examples.data.WineRecord

import scala.collection.JavaConverters._
import scala.collection._

/**
 * PMML model implementation for wine data.
 */
class WinePMMLModel(inputStream: Array[Byte]) extends PMMLModel[WineRecord, Double](inputStream) {

  /** Scoring (using PMML evaluator) */
  override def score(input: WineRecord): Double = {
    // Clear arguments (from previous run)
    arguments.clear()
    // Populate input based on record
    inputFields.asScala.foreach(field ⇒ {
      //      println("field-Name: " + field.getName + " Value: " + input.get(field.getName.getValue.replaceAll(" ", "_")))
      arguments.put(field.getName, field.prepare(input.get(field.getName.getValue.replaceAll(" ", "_"))))
    })

    // Calculate Output
    val result = evaluator.evaluate(arguments.asJava)

    //    println("Result: " + result)

    // Prepare output
    result.get(tname) match {
      case c: Computable ⇒ c.getResult.toString.toDouble
      case v: Any        ⇒ v.asInstanceOf[Double]
    }
  }

  // Support function to get values
  //  private def getValueByName(inputs: WineRecord, name: String): Double =
  //    WinePMMLModel.names.get(name) match {
  //      case Some(index) ⇒ {
  //        val v = inputs.getFieldByNumber(index + 1)
  //        v.asInstanceOf[Double]
  //      }
  //      case _ ⇒ .0
  //    }
}

/**
 * Factory for wine data PMML model
 */
object WinePMMLModel extends ModelFactory[WineRecord, Double] {
  private val names = Map(
    "fixed acidity" -> 0,
    "volatile acidity" -> 1, "citric acid" -> 2, "residual sugar" -> 3,
    "chlorides" -> 4, "free sulfur dioxide" -> 5, "total sulfur dioxide" -> 6,
    "density" -> 7, "pH" -> 8, "sulphates" -> 9, "alcohol" -> 10)

  override def create(input: ModelToServe): Option[Model[WineRecord, Double]] = {
    try {
      Some(new WinePMMLModel(input.model))
    } catch {
      case t: Throwable ⇒ None
    }
  }

  override def restore(bytes: Array[Byte]): Model[WineRecord, Double] = new WinePMMLModel(bytes)
}
