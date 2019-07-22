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

package com.lightbend.modelserving.model

import java.io.{ ObjectInputStream, ObjectOutputStream }

/**
 * Encapsulates a model to serve along with some metadata about it.
 * Using an Int for the modelType, instead of a ModelDescriptor.ModelType, which
 * is what it represents, is unfortunately necessary because otherwise you can't
 * use these objects in Spark UDFs; you get a Scala Reflection exception at runtime.
 * Hence, the integration values for modelType should match the known integer values
 * in the ModelType objects. See also protobufs/src/main/protobuf/modeldescriptor.proto
 */
final case class ModelMetadata(
  name: String,
  description: String,
  modelType: Int,
  modelBytes: Array[Byte] = Array.empty[Byte],
  location: Option[String] = None) {

  override def toString: String = {
    val sb = new StringBuilder
    sb.append("ModelMetadata(name = ").append(name)
      .append(", description = ").append(description)
      .append(", modelType = ").append(modelType)
      .append(", modelBytes = Array(...) of length ").append(modelBytes.length)
      .append(", location = ").append(location)
      .append(")")
      .toString
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case md: ModelMetadata ⇒
        name == md.name &&
          description == md.description &&
          modelType == md.modelType &&
          location == md.location &&
          arrayEquals(modelBytes, md.modelBytes)
      case _ ⇒ false
    }
  }

  private def arrayEquals(a1: Array[Byte], a2: Array[Byte]): Boolean =
    if (a1.length != a2.length) false
    else {
      for (i <- 0 until a1.length) {
        if (a1(i) != a2(i)) return false
      }
      true
    }

  /**
   * For cases where a non-empty name is needed, first try to use the name field,
   * but if it's empty, try to use the location name (i.e., after all directory
   * separators, e.g., "foo.txt" in "/bar/baz/foo.txt"). If the location is not
   * defined or empty, return "unnamed-model".
   */
  def constructName(): String = {
    if (name.length > 0) name
    else location match {
      case None | Some("") => "unnamed-model" // default hack
      case Some(path) =>
        val f = new java.io.File(path)
        f.getName
    }
  }

  // private def writeObject(output: ObjectOutputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   output.writeUTF(name)
  //   output.writeUTF(description)
  //   output.writeInt(modelType)
  //   output.writeObject(modelBytes)
  //   output.writeUTF(location.getOrElse(""))
  //   println(s"ModelMetadata serialization in ${System.currentTimeMillis() - start} ms")
  // }

  // private def readObject(input: ObjectInputStream): Unit = {
  //   val start = System.currentTimeMillis()
  //   name = input.readUTF()
  //   description = input.readUTF()
  //   modelType = input.readInt()
  //   bytes = input.readObject().asInstanceOf[Array[Byte]]
  //   val locationString = input.readUTF()
  //   location = if (locationString.length == 0) None else Some(locationString)
  //   try {
  //     println(s"ModelMetadata deserialization in ${System.currentTimeMillis() - start} ms")
  //   } catch {
  //     case t: Throwable ⇒
  //       throw new RuntimeException(
  //         s"ModelMetadata deserialization failed in ${System.currentTimeMillis() - start} ms", t)
  //   }
  // }
}

object ModelMetadata {
  /** Get the model from byte array */
  def apply(desc: ModelDescriptor, location: Option[String]): ModelMetadata = {
    val data = desc.modeldata match {
      case None => Array.empty[Byte]
      case Some(d) => d
    }
    new ModelMetadata(desc.name, desc.description, desc.modeltype.ordinal, data, location)
  }

  val unknown = new ModelMetadata(
    name = "unknown",
    description = "unknown description",
    modelType = -1)

  /**
   * Write an instance to a stream.
   */
  def write(metadata: ModelMetadata, output: ObjectOutputStream): Unit = {
    output.writeUTF(metadata.name)
    output.writeUTF(metadata.description)
    output.writeInt(metadata.modelType)
    output.writeObject(metadata.modelBytes)
    output.writeUTF(metadata.location.getOrElse(""))
  }

  /**
   * Read an instance from a stream.
   */
  def read(input: ObjectInputStream): ModelMetadata = {
    def loc() = {
      val locationString = input.readUTF()
      if (locationString.length == 0) None else Some(locationString)
    }
    new ModelMetadata(
      name = input.readUTF(),
      description = input.readUTF(),
      modelType = input.readInt(),
      modelBytes = input.readObject().asInstanceOf[Array[Byte]],
      location = loc())
  }
}
