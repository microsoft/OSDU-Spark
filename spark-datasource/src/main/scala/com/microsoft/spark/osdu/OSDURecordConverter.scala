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

import java.util.{ArrayList, List, Map}
import scala.collection.JavaConverters._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit


/** Convert OSDU schema to Spark SQL schema. */
class OSDURecordConverter(schema: StructType) {
  private val epoch = LocalDate.ofEpochDay(0)
  private val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm:ssZ]")

  private val logger = Logger.getLogger(classOf[OSDURecordConverter])

  /** Extracts the fields from the current OSDU record.
   *
   * @param record The OSDU record.
   * @return The internal row holding the extracted fields.
   */
  def toInternalRow(data: Map[String, Object]): InternalRow = InternalRow.fromSeq(toSeq(schema, data))

  def toJava(row: InternalRow): Map[String, Object] = toJava(row, schema)

  private def toJava(row: InternalRow, nestedSchema: StructType): Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()
    for (i <- 0 until row.numFields) {
      val fieldDataType = nestedSchema.fields(i).dataType

      if (!row.isNullAt(i)) {
        if (fieldDataType.isInstanceOf[ArrayType]) {
          // handle arrays
          val elementType = nestedSchema.fields(i).dataType.asInstanceOf[ArrayType].elementType

          val list = new ArrayList[Object]()
          val array = row.getArray(i)

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
        else if (fieldDataType.isInstanceOf[DateType]) {
          map.put(nestedSchema.fields(i).name, LocalDate.ofEpochDay(row.getInt(i)).atStartOfDay().format(dateFormat))
        }
        //        else if (fieldDataType.isInstanceOf[TimestampType])
        //          map.put(nestedSchema.fields(i).name, (row.getLong(i))))
        else if(fieldDataType.isInstanceOf[MapType]) {
          // convert to java map
          val sparkMap = row.getMap(i)

          map.put(
            nestedSchema.fields(i).name,
            ArrayBasedMapData.toJavaMap(
              sparkMap.keyArray().array,
              sparkMap.valueArray().array))
        }
        else
        // handle primitive types
          map.put(nestedSchema.fields(i).name, row.get(i, fieldDataType))
      }
    }

    map
  }

  private def numberToDouble(x: Any): Double = {
    x match {
      case d: Double => d
      case i: Int => i.toDouble
      case l: Long => l.toDouble
      case f: Float => f.toDouble
      case s: Short => s.toDouble
    }
  }

  /** Extracts the fields from the current OSDU record.
   *
   * @param nestedSchema The Spark SQL schema to follow.
   * @param nestedData The OSDU record to extract the fields from.
   * @return The data for the internal row.
   */
  private def toSeq(nestedSchema: StructType, nestedData: Map[String, Object]): Seq[Any] = {
    if (nestedData == null)
      Seq.empty
    else {
      nestedSchema.fields.map {
        field => {
          val fieldData = nestedData.get(field.name)

          if (fieldData == null) {
            // TODO: this could benefit from a good amount of unit testing
                        if (field.dataType.isInstanceOf[StructType] ||
                            field.dataType.isInstanceOf[ArrayType] ||
                            field.dataType.isInstanceOf[MapType] ||
                            field.dataType.isInstanceOf[DateType] ||
                            field.dataType.isInstanceOf[StringType]
                        )
                          null
                          //new ArrayBasedMapData(ArrayData.toArrayData(Array.empty), ArrayData.toArrayData(Array.empty))
                        else
                          Seq.empty
            null
          } else {
            field.dataType match {
              // primitive types
              case DataTypes.StringType => UTF8String.fromString(fieldData.asInstanceOf[String])
              case DataTypes.IntegerType => numberToDouble(fieldData).toInt
              case DataTypes.LongType => numberToDouble(fieldData).toLong
              case DataTypes.DoubleType => numberToDouble(fieldData)
              case DataTypes.FloatType => numberToDouble(fieldData).toFloat
              case DataTypes.ShortType => numberToDouble(fieldData).toInt
              //              case DataTypes.TimestampType => TODO: return long here
              case DataTypes.DateType => Option(fieldData.asInstanceOf[String]) match {
                case Some(s) => ChronoUnit.DAYS.between(epoch, LocalDate.parse(s, dateFormat)).toInt
                case _ => null
              }
              // complex types
              case _ => {
                if (field.dataType.isInstanceOf[StructType])
                // Recurse into nested fields
                // Nested records get their own internal row
                  InternalRow.fromSeq(
                    toSeq(
                      field.dataType.asInstanceOf[StructType],
                      fieldData.asInstanceOf[Map[String, Object]]))
                else if (field.dataType.isInstanceOf[MapType])
                  ArrayBasedMapData.apply(
                    fieldData.asInstanceOf[java.util.Map[String, Object]],
                    k => UTF8String.fromString(k.toString),
                    v => if (v != null) UTF8String.fromString(v.toString) else null)
                else if (field.dataType.isInstanceOf[ArrayType]) {
                  val arrType = field.dataType.asInstanceOf[ArrayType]

                  // process array
                  val elems = fieldData.asInstanceOf[List[Any]].asScala.map {
                    elem => {
                      arrType.elementType match {
                        // primitive types
                        case DataTypes.StringType => UTF8String.fromString(elem.asInstanceOf[String])
                        case DataTypes.IntegerType => numberToDouble(elem).toInt
                        case DataTypes.LongType => numberToDouble(elem).toLong
                        case DataTypes.DoubleType => numberToDouble(elem)
                        case DataTypes.FloatType => numberToDouble(elem).toFloat
                        case DataTypes.ShortType => numberToDouble(elem).toInt
                        case DataTypes.DateType => Option(elem.asInstanceOf[String]) match {
                          // Internally, a date is stored as a simple incrementing count of days
                          // where day 0 is 1970-01-01. Negative numbers represent earlier days.
                          // https://github.com/apache/spark/blob/09b789a17efc6788176debd1925ff0fb78910e9f/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DateType.scala#L35
                          case Some(s) => ChronoUnit.DAYS.between(epoch,  LocalDate.parse(s, dateFormat)).toInt
                          case _ => null
                        }
                        // recurse into nested fields
                        case _ => toSeq(elem.asInstanceOf[StructType], elem.asInstanceOf[Map[String, Object]])
                      }
                    }
                  }

                  // create Spark SQL ArrayData
                  ArrayData.toArrayData(elems.toArray)
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
  }
}