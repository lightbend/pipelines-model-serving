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

object ModelDescriptorUtil {

  object implicits {
    /**
     * Utilities for the Avro-generated ModelDescriptor.
     * Note that not using an Int for the modelType, may cause problems trying to use
     * this code and ModelDescriptor in Spark UDFs; you get a Scala Reflection exception
     * at runtime. This will be addressed in a future release.
     */
    implicit class RichModelDescriptor(descriptor: ModelDescriptor) {

      def toRichString: String = {
          def bytesLen(): Int =
            if (descriptor.modelBytes == None) 0
            else descriptor.modelBytes.get.length

        val sb = new StringBuilder
        sb.append("ModelDescriptor(name = ").append(descriptor.name)
          .append(", description = ").append(descriptor.description)
          .append(", modelType = ").append(descriptor.modelType)
          .append(", dataType = ").append(descriptor.dataType)
          .append(", modelBytes = ").append(descriptor.modelBytes.slice(0, 128).toString)
          .append(" of length ").append(bytesLen())
          .append(", modelSourceLocation = ").append(descriptor.modelSourceLocation)
          .append(")")
          .toString
      }

      override def equals(obj: Any): Boolean = {
        obj match {
          case md: ModelDescriptor ⇒
            descriptor.name == md.name &&
              descriptor.description == md.description &&
              descriptor.modelType == md.modelType &&
              descriptor.dataType == md.dataType &&
              descriptor.modelSourceLocation == md.modelSourceLocation &&
              arrayEquals(descriptor.modelBytes, md.modelBytes)
          case _ ⇒ false
        }
      }

      private def arrayEquals(oa1: Option[Array[Byte]], oa2: Option[Array[Byte]]): Boolean =
        (oa1, oa2) match {
          case (None, None)          ⇒ true
          case (_, None) | (None, _) ⇒ false
          case (Some(a1), Some(a2)) ⇒
            if (a1.length != a2.length) false
            else {
              for (i ← 0 until a1.length) {
                if (a1(i) != a2(i)) return false
              }
              true
            }
        }

      /**
       * For cases where a non-empty name is needed, first try to use the name field,
       * but if it's empty, try to use the location name (i.e., after all directory
       * separators, e.g., "foo.txt" in "/bar/baz/foo.txt"). If the location is not
       * defined or empty, return "unnamed-model".
       */
      def constructName(): String = {
        if (descriptor.name.length > 0) descriptor.name
        else descriptor.modelSourceLocation match {
          case None | Some("") ⇒ "unnamed-model" // default hack
          case Some(path) ⇒
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
      //   output.writeUTF(modelSourceLocation.getOrElse(""))
      //   println(s"ModelDescriptorUtil serialization in ${System.currentTimeMillis() - start} ms")
      // }

      // private def readObject(input: ObjectInputStream): Unit = {
      //   val start = System.currentTimeMillis()
      //   name = input.readUTF()
      //   description = input.readUTF()
      //   modelType = input.readInt()
      //   bytes = input.readObject().asInstanceOf[Array[Byte]]
      //   val locationString = input.readUTF()
      //   modelSourceLocation = if (locationString.length == 0) None else Some(locationString)
      //   try {
      //     println(s"ModelDescriptorUtil deserialization in ${System.currentTimeMillis() - start} ms")
      //   } catch {
      //     case t: Throwable ⇒
      //       throw new RuntimeException(
      //         s"ModelDescriptorUtil deserialization failed in ${System.currentTimeMillis() - start} ms", t)
      //   }
      // }
    }
  }

  val unknown = ModelDescriptor(
    name = "unknown",
    description = "unknown description",
    modelType = ModelType.PMML, // arbitrary
    dataType = "",
    modelBytes = None,
    modelSourceLocation = None)

  /**
   * Write an instance to a stream.
   */
  def write(descriptor: ModelDescriptor, output: ObjectOutputStream): Unit = {
    output.writeUTF(descriptor.name)
    output.writeUTF(descriptor.description)
    output.writeObject(descriptor.modelType)
    output.writeUTF(descriptor.dataType)
    output.writeObject(
      if (descriptor.modelBytes == None) Array.empty[Byte] else descriptor.modelBytes.get)
    output.writeUTF(descriptor.modelSourceLocation.getOrElse(""))
  }

  /**
   * Read an instance from a stream.
   */
  def read(input: ObjectInputStream): ModelDescriptor = {
      def loc() = {
        val locationString = input.readUTF()
        if (locationString.length == 0) None else Some(locationString)
      }
      def bytes() = {
        val bs = input.readObject().asInstanceOf[Array[Byte]]
        if (bs.length == 0) None else Some(bs)
      }
    val name = input.readUTF()
    val description = input.readUTF()
    val modelType = input.readObject().asInstanceOf[ModelType]
    val dataType = input.readUTF()
    val modelBytes = bytes()
    val location = loc()
    ModelDescriptor(name, description, dataType, modelType, modelBytes, location)
  }
}
