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

import java.io.IOException
import java.util.Map

import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructType}
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConverters._
import scala.collection.mutable.Queue
import org.apache.spark.sql.catalyst.util.ArrayData

import osdu.client.{ApiClient, Configuration}
import osdu.client.api.SearchApi
import osdu.client.model.SearchCursorQueryRequest

@SerialVersionUID(1L)
class OSDUInputPartitionReader(kind: String, query: String, oakApiEndpoint: String, partitionId: String, bearerToken: String, schema: StructType)
  extends InputPartitionReader[InternalRow] with Serializable {

  private val logger = Logger.getLogger(classOf[OSDUInputPartitionReader])

  private var currentRow: InternalRow = _

  private val client = new ApiClient()

  client.setBasePath(oakApiEndpoint)
  client.setApiKey(bearerToken)
  client.setApiKeyPrefix("Bearer")
  client.addDefaultHeader("data-partition-id", partitionId)

  private val searchApi = new SearchApi(client)
  private val queryRequest = new SearchCursorQueryRequest()
  // private var offset: Int = 0 // TODO: could be used to parallize across multiple nodes
  private val localBuffer = new Queue[java.util.Map[String, Object]]

  queryRequest.kind(kind)
  queryRequest.query(query)
  // queryRequest.offset(offset)
  queryRequest.limit(1000) // TODO: add parameter

  private def schemaToPaths(nestedSchema: StructType, parent: Seq[String]): Seq[String] = {
    nestedSchema.fields.flatMap {
      field => {
        val fieldPath = (parent :+ field.name)
        if (field.dataType.isInstanceOf[StructType])
          schemaToPaths(field.dataType.asInstanceOf[StructType], fieldPath)
        if (field.dataType.isInstanceOf[ArrayType]) {
          val arrType = field.dataType.asInstanceOf[ArrayType]

          if (arrType.elementType.isInstanceOf[StructType])
            schemaToPaths(arrType.elementType.asInstanceOf[StructType], fieldPath)
          else
            Seq(fieldPath.mkString("."))
        }
        else
          Seq(fieldPath.mkString("."))
      }
    }
  }

  // println(schemaToPaths(schema, Seq()).mkString("\n"))

  queryRequest.setReturnedFields(schemaToPaths(schema, Seq()).asJava)


  override def close(): Unit = { }

  @IOException
  override def next: Boolean = {

    if (localBuffer.isEmpty) {
      // TODO API needs cleanup
      val result = searchApi.queryWithCursor("foo", queryRequest, null);

      if (result.getResults.size == 0)
        return false

      localBuffer ++= result.getResults.asScala

      // TOOD: useful for debugging to see raw response
      // println(result.getResults.get(0))

      // use cursor for next request
      queryRequest.setCursor(result.getCursor())
    }

    def mapToNestedArray(nestedSchema: StructType, nestedData: Map[String, Object]): Array[Any] = {
      nestedSchema.fields.map {
        field => {
          val fieldData = nestedData.get(field.name)

          field.dataType match {
            // primitive types
            case DataTypes.StringType => UTF8String.fromString(fieldData.asInstanceOf[String])
            case DataTypes.IntegerType => fieldData.asInstanceOf[Double].toInt
            // complex types
            case _ => {
              // struct handling
              if (field.dataType.isInstanceOf[StructType])
                new GenericInternalRow(mapToNestedArray(
                  field.dataType.asInstanceOf[StructType], 
                  fieldData.asInstanceOf[Map[String, Object]]))
              // array handling
              else if (field.dataType.isInstanceOf[ArrayType]) {
                val arrType = field.dataType.asInstanceOf[ArrayType]
                // println(field.name)
                // println(nestedData)

                if (fieldData == null)
                  // TODO: all arrays are nullable, but passing null doesn't work
                  ArrayData.toArrayData(new Array[Any](0))
                else {
                  val elems = fieldData.asInstanceOf[List[Any]].map {
                    elem => {
                      arrType.elementType match {
                        case DataTypes.StringType => UTF8String.fromString(elem.asInstanceOf[String])
                        case DataTypes.IntegerType => elem.asInstanceOf[Double].toInt
                        case _ => mapToNestedArray(elem.asInstanceOf[StructType], fieldData.asInstanceOf[Map[String, Object]])
                      }
                    }
                  }

                  ArrayData.toArrayData(elems.toArray)
                }
              }
              else
                fieldData.asInstanceOf[Any]
            }
          }
        }
      }
    }

    val data = localBuffer.dequeue().asInstanceOf[Map[String, Object]]
    currentRow = new GenericInternalRow(mapToNestedArray(schema, data))

    return true
  }

  override def get(): InternalRow = currentRow
}