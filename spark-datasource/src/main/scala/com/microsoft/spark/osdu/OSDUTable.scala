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

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{Write, LogicalWriteInfo, PhysicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.microsoft.osdu.client.api.SchemaApi
import com.microsoft.osdu.client.invoker.ApiClient

import scala.collection.JavaConverters._
import java.util.Map

class OSDUTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String])
  extends Table with SupportsRead with SupportsWrite {

  private val kind = map.get("kind")
  private val query = map.get("query")
  private val osduApiEndpoint = map.get("osduApiEndpoint")
  private val partitionId = map.get("partitionId")
  private val bearerToken = map.get("bearerToken")

  override def name(): String = kind

  override def schema(): StructType = {

    if (structType != null)
      structType
    else {
      // setup REST client
      val client = new ApiClient()

      client.setBasePath(osduApiEndpoint)
      client.setBearerToken(bearerToken)

      val schemaApi = new SchemaApi(client)

      // fetch OSDU schema from service
      val schema = schemaApi.getSchema(partitionId, kind).asInstanceOf[Map[String, Object]]

      // convert OSDU schema to Spark SQL schema
      OSDUSchemaConverter.toStruct(schema)
    }
  }

  override def capabilities(): util.Set[TableCapability] = 
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = new OSDUScanBuilder(options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new OSDUWriteBuilder(info)
}

class OSDUScanBuilder(options: CaseInsensitiveStringMap) extends ScanBuilder with SupportsPushDownRequiredColumns {
  var prunedSchema: Option[StructType] = None

  override def build(): Scan = new OSDUScan(options, prunedSchema)

  override def pruneColumns(requiredSchema: StructType): Unit =  {
    prunedSchema = Some(requiredSchema)
  }
}

class SimplePhysicalWriteInfo extends PhysicalWriteInfo {
  override def numPartitions(): Int = 1
}

class OSDUWriteBuilder(info: LogicalWriteInfo) extends WriteBuilder {
 override def build(): Write = new OSDUWrite(info, new SimplePhysicalWriteInfo())
}