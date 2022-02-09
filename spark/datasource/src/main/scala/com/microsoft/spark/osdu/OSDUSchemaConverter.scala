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
import java.util.{ArrayList, Map, List, UUID}
import scala.collection.JavaConverters._

/** Convert OSDU schema to Spark SQL schema. */
class OSDUSchemaConverter(definitions: Map[String, Map[String, Object]]) {
    private val logger = Logger.getLogger(classOf[OSDUSchemaConverter])

    private def resolveDataType(subObj: Map[String, Object]): Option[DataType] = {
      subObj.get("type").asInstanceOf[String] match {
        case "string"  => Some(StringType)
        case "boolean" => Some(BooleanType)
        case "integer" => Some(IntegerType)
        case "number"  => Some(DoubleType)
        case "double"  => Some(DoubleType)
        case "float"   => Some(DoubleType)
        // recurse into nested schema
        case "object"  => osduSchemaToStruct(subObj)
        case "array"   => { 
          val itemData = subObj.get("items").asInstanceOf[Map[String, Object]]
          // resolve primitive & complex types
          val elemType = resolveDataType(itemData).getOrElse({
            // resolve reference types
            val refStr = itemData.get("$ref").asInstanceOf[String].substring("#/definitions/".length)
            // recurse into nested schema
            osduSchemaToStruct(definitions.get(refStr)).get
          })
          Some(ArrayType(elemType, true))
        }
        // if "type" string is not defined, it's a nested schema
        case null => osduSchemaToStruct(subObj)
        case _ => {
            logger.warn(s"Unsupported type: ${subObj.get("type")}")
            None
        }
      }
    }

    /** Convert the OSDU schema to a Spark SQL schema.
     *
     * Any polymorphic types are fully expanded.
     */
    def osduSchemaToStruct(obj: Map[String, Object]): Option[StructType] = {
      // println(s"osduSchemaToStruct $obj") // use for debugging

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

        if (axxOf == null)
          None
        else {
          // union all properties of objects contained in allOf list
          val subFields = axxOf.asScala.flatMap { osduSchemaToStruct(_).map { _.fields } }
          // flatten - distinctBy name
          Some(new StructType(subFields.flatten.toList.groupBy(_.name).map(_._2.head).toArray))
        }
      }
      else {
        val fields = props.flatMap { 
          case (propertyName, obj) => {
            // resolve schema reference
            var refStr = obj.get("$ref").asInstanceOf[String];
            if (refStr != null) {
              // inject definition
              osduSchemaToStruct(definitions.get(refStr.substring("#/definitions/".length))).get.fields
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

              resolvedDataType.map { StructField(propertyName, _, isNullable, metadata).withComment(comment) }
            }
          }
        }
        // Scala 2.13.x
        // Some(new StructType(fields.toSeq.distinctBy { _.name } .toArray))

        // Scala 2.12.x
        Some(new StructType(fields.toList.groupBy(_.name).map(_._2.head).toArray))
      }
    }
}

object OSDUSchemaConverter {

  /** Convert OSDU schema definition to Spark SQL struct */
  def toStruct(schema: Map[String, Object]): StructType = {
    // definitions are top-level
    val definitions = schema.get("definitions").asInstanceOf[Map[String, Map[String, Object]]]

    // run converter
    new OSDUSchemaConverter(definitions).osduSchemaToStruct(schema).get
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