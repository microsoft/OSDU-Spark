/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.osdu

import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.apache.spark.sql.types

import java.util.{ArrayList, List, Map}
import scala.collection.JavaConverters._

/** Convert OSDU schema to Spark SQL schema. */
class OSDUSchemaConverter(definitions: Map[String, Map[String, Object]]) {
  private val logger = Logger.getLogger(classOf[OSDUSchemaConverter])

  private def resolveDataType(subObj: Map[String, Object]): DataType = {
    subObj.get("type").asInstanceOf[String] match {
      case "string"  => StringType
      case "boolean" => BooleanType
      case "integer" => IntegerType
      case "number"  => DoubleType
      case "double"  => DoubleType
      case "float"   => DoubleType
      case "date"    => DateType
      // recurse into nested schema
      case "object"  => osduSchemaToDataType(subObj)
      case "array"   => {
        val itemData = subObj.get("items").asInstanceOf[Map[String, Object]]
        // resolve primitive & complex types
        val elemType = resolveDataType(itemData)

        //            .getOrElse({
        //            // resolve reference types
        //            val refStr = itemData.get("$ref").asInstanceOf[String].substring("#/definitions/".length)
        //            // recurse into nested schema
        //            osduSchemaToStruct(definitions.get(refStr))
        //          })
        ArrayType(elemType, true)
      }
      // if "type" string is not defined, it's a nested schema
      case "null" => NullType
      case null => osduSchemaToDataType(subObj)
      case _ => throw new IllegalArgumentException(s"Unsupported type: ${subObj.get("type")}")
    }
  }

  /** Convert the OSDU schema to a Spark SQL schema.
   *
   * Any polymorphic types are fully expanded.
   */
  def osduSchemaToDataType(obj: Map[String, Object]): DataType = {
    //      println(s"osduSchemaToStruct $obj") // use for debugging

    // fetch mandatory fields and translate to nullable flag
    val required = obj.getOrDefault("required", new ArrayList[String]()).asInstanceOf[List[String]]

    val props = obj.get("properties").asInstanceOf[Map[String, Map[String, Object]]].asScala
    if (props == null) {
      // from a global/union perpsective allOf/anyOf has the same behavior
      var axxOf = obj.get("allOf").asInstanceOf[List[Map[String, Object]]]
      if (axxOf == null)
        axxOf = obj.get("anyOf").asInstanceOf[List[Map[String, Object]]]
      if (axxOf == null)
        axxOf = obj.get("oneOf").asInstanceOf[List[Map[String, Object]]]

      if (axxOf == null) {
        if (obj.containsKey("$ref")) {
          val refStr = obj.get("$ref").asInstanceOf[String].substring("#/definitions/".length)
          // recurse into nested schema
          osduSchemaToDataType(definitions.get(refStr))
        }
        else
          MapType(StringType, StringType)
      } else {
        // union all properties of objects contained in allOf list
        val subFields: Seq[StructField] = axxOf.asScala
          .map { osduSchemaToDataType(_)  }
          .filter { _.isInstanceOf[StructType] } // remove map types (can't union them)
          .map { _.asInstanceOf[StructType].fields }
          .flatten

        // flatten - distinctBy name
        new StructType(subFields.toList.groupBy(_.name).map(_._2.head).toArray)
      }
    }
    else {
      val fields = props.map {
        case (propertyName, obj) => {
          // resolve schema reference
          var refStr = obj.get("$ref").asInstanceOf[String];
          if (refStr != null) {
            // inject definition
            Seq(
              StructField(
                propertyName,
                osduSchemaToDataType(
                  definitions.get(refStr.substring("#/definitions/".length)))))
          }
          else {
            // in-place schema definition
            val resolvedDataType = resolveDataType(obj)
            val comment = obj.get("description").asInstanceOf[String]
            val isNullable = !required.contains(propertyName)

            // TODO: we could propagate the metadata
            val metadata = new MetadataBuilder()
              .putString("pattern", obj.get("pattern").asInstanceOf[String])
              .putString("title",   obj.get("title").asInstanceOf[String])
              // TODO: x-osdu-relationship
              .build()

            Seq(StructField(propertyName, resolvedDataType, isNullable, metadata).withComment(comment))
          }
        }
      }.flatten
      // Scala 2.13.x
      // Some(new StructType(fields.toSeq.distinctBy { _.name } .toArray))

      // Scala 2.12.x
      new StructType(fields.toList.groupBy(_.name).map(_._2.head).toArray)
    }
  }
}

object OSDUSchemaConverter {

  /** Convert OSDU schema definition to Spark SQL struct */
  def toDataType(schema: Map[String, Object]): DataType = {
    // definitions are top-level
    val definitions = schema.get("definitions").asInstanceOf[Map[String, Map[String, Object]]]

    // run converter
    new OSDUSchemaConverter(definitions).osduSchemaToDataType(schema)
  }

  /** Maps a Spark SQL schema to JSON-like paths
   *
   * @param schema Spark SQL schema.
   * @return Sequence of all fields in dot-notation (e.g. a.b.c, d.e, ...)
   */
  def schemaToPaths(schema: StructType): Seq[String] = schemaToPaths(schema, Seq())

  /** Maps a Spark SQL schema to JSON-like paths
   *
   * @param nestedSchema Spark SQL schema.
   * @param parent Sequence of parent fields.
   * @return Sequence of all fields in dot-notation (e.g. a.b.c, d.e, ...)
   */
  private def schemaToPaths(nestedSchema: StructType, parent: Seq[String]): Seq[String] = {
    nestedSchema.fields.flatMap {
      field => {
        val fieldPath = (parent :+ field.name)
        if (field.dataType.isInstanceOf[StructType]) {
          // Recurse into nested fields
          schemaToPaths(field.dataType.asInstanceOf[StructType], fieldPath)
        }
        else if (field.dataType.isInstanceOf[ArrayType]) {
          // expand arrays
          val arrType = field.dataType.asInstanceOf[ArrayType]

          if (arrType.elementType.isInstanceOf[StructType])
          // Recurse into nested fields
            schemaToPaths(arrType.elementType.asInstanceOf[StructType], fieldPath)
          else
            Seq(fieldPath.mkString("."))
        }
        else
        // Leaf node
          Seq(fieldPath.mkString("."))
      }
    }
  }
}