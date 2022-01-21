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
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import java.util.{ArrayList, Map, List, UUID}
import scala.collection.JavaConverters._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData

/** Convert OSDU schema to Spark SQL schema. */
class OSDURecordConverter(schema: StructType) {
  private val logger = Logger.getLogger(classOf[OSDURecordConverter])

  /** Extracts the fields from the current OSDU record.
    *
    * @param record The OSDU record.
    * @return The internal row holding the extracted fields.
    */
  def toInternalRow(data: Map[String, Object]): InternalRow = InternalRow.fromSeq(toSeq(schema, data))

  def toJava(row: InternalRow): Map[String, Object] = toJava(row, schema)

  // private def toSeq(nestedSchema: StructType, nestedData: Map[String, Object]): Seq[Any] = {
  private def toJava(row: InternalRow, nestedSchema: StructType): Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()
    for (i <- 0 until row.numFields) {
      val fieldDataType = nestedSchema.fields(i).dataType

      if (fieldDataType.isInstanceOf[ArrayType]) {
        // handle arrays
        val elementType = nestedSchema.fields(i).dataType.asInstanceOf[ArrayType].elementType
        
        val list = new ArrayList[Object]()
        val array = row.getArray(i).asInstanceOf[ArrayData]

        if (elementType.isInstanceOf[StructType]) {
          // handle structs in arrays
          val elementTypeAsStruct = elementType.asInstanceOf[StructType]

          for (j <- 0 until array.numElements()) {
            list.add(
              toJava(
                array.getStruct(j, elementTypeAsStruct.size),
                elementTypeAsStruct))
          }
          map.put(nestedSchema.fields(i).name, list)
        }
        else {
          // handle all other types
          if (array != null) {
            for (j <- 0 until array.numElements())
              list.add(array.get(j, nestedSchema.fields(i).dataType.asInstanceOf[ArrayType].elementType))
            
            map.put(nestedSchema.fields(i).name, list)
          }
        }
      }
      else if (fieldDataType.isInstanceOf[StructType]) {
        // handle nested structs and recurse
        map.put(
            nestedSchema.fields(i).name,
            toJava(
              row.getStruct(
                i,
                nestedSchema.fields(i).dataType.asInstanceOf[StructType].size),
              nestedSchema.fields(i).dataType.asInstanceOf[StructType]))
      }
      else
        // handle primitive types
        map.put(nestedSchema.fields(i).name, row.get(i, fieldDataType))
    }

    map
  }

  /** Extracts the fields from the current OSDU record.
   *
   * @param nestedSchema The Spark SQL schema to follow.
   * @param nestedData The OSDU record to extract the fields from.
   * @return The data for the internal row.
   */
  private def toSeq(nestedSchema: StructType, nestedData: Map[String, Object]): Seq[Any] = {
    nestedSchema.fields.map {
      field => {
        val fieldData = nestedData.get(field.name)

        field.dataType match {
          // primitive types
          case DataTypes.StringType  => fieldData.asInstanceOf[String]
          case DataTypes.IntegerType => fieldData.asInstanceOf[Double].toInt
          case DataTypes.DoubleType  => fieldData.asInstanceOf[Double]
          // complex types
          case _ => {
            if (field.dataType.isInstanceOf[StructType])
              // Recurse into nested fields
              // Nested records get their own internal row
              InternalRow.fromSeq(
                toSeq(
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
                      case DataTypes.StringType  => elem.asInstanceOf[String]
                      case DataTypes.IntegerType => elem.asInstanceOf[Double].toInt
                      case DataTypes.DoubleType  => elem.asInstanceOf[Double]
                      // recurse into nested fields
                      case _ => toSeq(elem.asInstanceOf[StructType], fieldData.asInstanceOf[Map[String, Object]])
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