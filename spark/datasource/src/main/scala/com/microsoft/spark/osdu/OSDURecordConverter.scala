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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import java.util.{ArrayList, Map, List, UUID}
import scala.collection.JavaConverters._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData

/** Convert OSDU schema to Spark SQL schema. */
class OSDURecordConverter(schema: StructType) {
    private val logger = Logger.getLogger(classOf[OSDURecordConverter])

    def toInternalRow(data: Map[String, Object]): InternalRow = new GenericInternalRow(toNestedArray(schema, data))

    /** Extracts the fields from the current OSDU record.
     *
     * @param nestedSchema The Spark SQL schema to follow.
     * @param nestedData The OSDU record to extract the fields from.
     * @return The data for the internal row.
     */
    private def toNestedArray(nestedSchema: StructType, nestedData: Map[String, Object]): Array[Any] = {
      nestedSchema.fields.map {
        field => {
          val fieldData = nestedData.get(field.name)

          field.dataType match {
            // primitive types
            case DataTypes.StringType  => UTF8String.fromString(fieldData.asInstanceOf[String])
            case DataTypes.IntegerType => fieldData.asInstanceOf[Double].toInt
            case DataTypes.DoubleType  => fieldData.asInstanceOf[Double].toInt
            // complex types
            case _ => {
              if (field.dataType.isInstanceOf[StructType])
                // Recurse into nested fields
                // Nested records get their own internal row
                new GenericInternalRow(
                  toNestedArray(
                    field.dataType.asInstanceOf[StructType], 
                    fieldData.asInstanceOf[Map[String, Object]]))
              else if (field.dataType.isInstanceOf[ArrayType]) {
                val arrType = field.dataType.asInstanceOf[ArrayType]

                if (fieldData == null)
                  // Empty array
                  // TODO: all arrays are nullable, but passing null doesn't work
                  ArrayData.toArrayData(new Array[Any](0))
                else {
                  // process array
                  val elems = fieldData.asInstanceOf[List[Any]].asScala.map {
                    elem => {
                      arrType.elementType match {
                        // primitive types
                        case DataTypes.StringType  => UTF8String.fromString(elem.asInstanceOf[String])
                        case DataTypes.IntegerType => elem.asInstanceOf[Double].toInt
                        case DataTypes.DoubleType  => elem.asInstanceOf[Double].toInt
                        // recurse into nested fields
                        case _ => toNestedArray(elem.asInstanceOf[StructType], fieldData.asInstanceOf[Map[String, Object]])
                      }
                    }
                  }

                  // create Spark SQL ArrayData
                  ArrayData.toArrayData(elems.toArray)
                }
              }
              else
                // Fallback
                fieldData.asInstanceOf[Any]
            }
          }
        }
      }
    }
}