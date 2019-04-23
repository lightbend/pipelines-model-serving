///*
// * Copyright (C) 2017-2019  Lightbend
// *
// * This file is part of the Lightbend model-serving-tutorial (https://github.com/lightbend/model-serving-tutorial)
// *
// * The model-serving-tutorial is free software: you can redistribute it and/or modify
// * it under the terms of the Apache License Version 2.0.
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.lightbend.modelserving.winemodel.tensorflow
//
//import com.lightbend.modelserving.model.tensorflow.TensorFlowBundleModel
//import com.lightbend.modelserving.model.{ Model, ModelFactory }
//import com.lightbend.modelserving.model.ModelToServe
//import pipelines.examples.data.WineRecord
//
///**
// * Implementation of TensorFlow bundled model for Wine.
// */
//class WineTensorFlowBundledModel(inputStream: Array[Byte]) extends TensorFlowBundleModel[WineRecord, Double](inputStream) {
//
//  /**
//   * Score data.
//   *
//   * @param input object to score.
//   * @return scoring result
//   */
//  override def score(input: WineRecord): Double = {
//    // Create input tensor
//    val modelInput = WineTensorFlowModel.toTensor(input)
//    // Serve model using TensorFlow APIs
//    val signature = signatures.head._2
//    val tinput = signature.inputs.head._2
//    val toutput = signature.outputs.head._2
//    val result = session.runner.feed(tinput.name, modelInput).fetch(toutput.name).run().get(0)
//    // process result
//    val rshape = result.shape
//    var rMatrix = Array.ofDim[Float](rshape(0).asInstanceOf[Int], rshape(1).asInstanceOf[Int])
//    result.copyTo(rMatrix)
//    rMatrix(0).indices.maxBy(rMatrix(0)).toDouble
//  }
//}
//
///**
// * Implementation of TensorFlow bundled model factory.
// */
//object WineTensorFlowBundledModel extends ModelFactory[WineRecord, Double] {
//
//  /**
//   * Creates a new TensorFlow bundled model.
//   *
//   * @param descriptor model to serve representation of PMML model.
//   * @return model
//   */
//  override def create(input: ModelToServe): Option[Model[WineRecord, Double]] =
//    try
//      Some(new WineTensorFlowBundledModel(input.location.getBytes))
//    catch {
//      case t: Throwable ⇒ None
//    }
//
//  /**
//   * Restore PMML model from binary.
//   *
//   * @param bytes binary representation of PMML model.
//   * @return model
//   */
//  override def restore(bytes: Array[Byte]): Model[WineRecord, Double] = new WineTensorFlowBundledModel(bytes)
//}
//
