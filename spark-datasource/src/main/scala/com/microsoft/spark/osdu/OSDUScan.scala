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
import org.apache.spark.sql.connector.read
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.microsoft.osdu.client.api.SchemaApi
import com.microsoft.osdu.client.invoker.ApiClient

import java.util.Map

class OSDUScan(options: CaseInsensitiveStringMap, prunedSchema: Option[StructType]) extends Scan with Batch {

  // TODO: refactor
  private val kind = options.get("kind")
  private val osduApiEndpoint = options.get("osduApiEndpoint")
  private val partitionId = options.get("partitionId")
  private val bearerToken = options.get("bearerToken")

  private lazy val schemaForKind = {
    // setup REST client
    val client = new ApiClient()

    // Configure HTTP bearer authorization: Bearer
    client.setBasePath(osduApiEndpoint)
    client.setBearerToken(bearerToken)

    val schemaApi = new SchemaApi(client)

    // fetch OSDU schema from service
    val schema = schemaApi.getSchema(partitionId, kind).asInstanceOf[Map[String, Object]]

    // convert OSDU schema to Spark SQL schema
    OSDUSchemaConverter.toStruct(schema)
  }

  override def readSchema(): StructType = prunedSchema.getOrElse(schemaForKind)

  override def planInputPartitions(): Array[InputPartition] = Array(new OSDUPartition())

  override def createReaderFactory(): read.PartitionReaderFactory = new OSDUPartitionReaderFactory(options, readSchema)
}

class OSDUPartition extends InputPartition

class OSDUPartitionReaderFactory(options: CaseInsensitiveStringMap, prunedSchema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new OSDUPartitionReader(options, prunedSchema)
}
