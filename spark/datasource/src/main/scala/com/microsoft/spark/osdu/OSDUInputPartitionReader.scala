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

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StructType}
import scala.collection.JavaConverters._
import scala.collection.mutable.Queue

import osdu.client.{ApiClient, Configuration}
import osdu.client.api.SearchApi
import osdu.client.model.SearchCursorQueryRequest

/**
  * A [[InputPartitionReader]] for reading data from a [[SearchApi]].
  *
  * @param kind The OSDU record kind.
  * @param query The OSDU query string.
  * @param oakApiEndpoint HTTPS endpoint of the Oak API.
  * @param partitionId The OSDU partition id.
  * @param bearerToken The authentication bearer token.
  * @param schema The pruned schema of the data.
  */
@SerialVersionUID(1L)
class OSDUInputPartitionReader(kind: String, query: String, oakApiEndpoint: String, partitionId: String, bearerToken: String, schema: StructType)
  extends InputPartitionReader[InternalRow] with Serializable {

  private val logger = Logger.getLogger(classOf[OSDUInputPartitionReader])

  private var currentRow: InternalRow = _

  private val osduRecordConverter = new OSDURecordConverter(schema)

  // setup REST client
  private val client = new ApiClient()

  client.setBasePath(oakApiEndpoint)
  client.setApiKey(bearerToken)
  client.setApiKeyPrefix("Bearer")
  client.addDefaultHeader("data-partition-id", partitionId)

  private val searchApi = new SearchApi(client)
  private val queryRequest = new SearchCursorQueryRequest()
  private val localBuffer = new Queue[java.util.Map[String, Object]]

  queryRequest.kind(kind)
  queryRequest.query(query)
 // TODO: expose to API surface
  queryRequest.limit(1000)

  // TODO: could be used to parallize across multiple nodes - while sacrificing consistency
  // private var offset: Int = 0 
  // queryRequest.offset(offset)

    // prune schema to only include fields that are used in the query
  queryRequest.setReturnedFields(OSDUSchemaConverter.schemaToPaths(schema).asJava)

  override def close(): Unit = { }

  /** Advance to the next row. 
    * 
    * Uses a simple queue interally to buffer the batch records returned by the API.
    *
    * @return True if there is a next row, false otherwise.
    */
  @IOException
  override def next: Boolean = {

    if (localBuffer.isEmpty) {
      // TODO API needs cleanup
      val result = searchApi.queryWithCursor("foo", queryRequest, null);

      if (result.getResults.size == 0)
        // No more records
        return false

      // append batch to local buffer
      localBuffer ++= result.getResults.asScala

      // TODO: useful for debugging to see raw response
      // println(result.getResults.get(0))

      // use cursor for next request
      // service keeps the cursor open for 5 minutes
      queryRequest.setCursor(result.getCursor())
    }

    // get the next row
    val data = localBuffer.dequeue().asInstanceOf[Map[String, Object]]

    // extract data from OSDU record structure
    currentRow = osduRecordConverter.toInternalRow(data)

    return true
  }

  override def get(): InternalRow = currentRow
}