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
import org.apache.spark.sql.types.{StructType}

import java.util
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.microsoft.osdu.client.api.SchemaApi
import com.microsoft.osdu.client.invoker.ApiClient

import scala.collection.JavaConverters._
import java.util.Map

class OSDUTable(structType: StructType, transforms: Array[Transform], val query: String, osduOptions: OSDUOptions)
  extends Table with SupportsRead with SupportsWrite {

  override def name(): String = osduOptions.kind

  override def schema(): StructType = {

    if (structType != null)
      structType
    else {
      // setup REST client
      val client = new ApiClient()

      client.setBasePath(osduOptions.osduApiEndpoint)
      client.setBearerToken(osduOptions.getBearerToken)

      val schemaApi = new SchemaApi(client)

      // fetch OSDU schema from service
      val schema = schemaApi.getSchema(osduOptions.partitionId, osduOptions.kind).asInstanceOf[Map[String, Object]]

      // convert OSDU schema to Spark SQL schema
      OSDUSchemaConverter.toDataType(schema).asInstanceOf[StructType]
    }
  }

  override def capabilities(): util.Set[TableCapability] =
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.STREAMING_WRITE,
      TableCapability.ACCEPT_ANY_SCHEMA).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    // propagate credentials

    val newOptions = new java.util.HashMap[String, String]
    newOptions.put("osduApiEndpoint", osduOptions.osduApiEndpoint)
    newOptions.put("partitionId", osduOptions.partitionId)
    newOptions.put("bearerToken", osduOptions.getBearerToken)
    newOptions.put("kind", osduOptions.kind)

    newOptions.putAll(options)

    new OSDUScanBuilder(new CaseInsensitiveStringMap(newOptions))
  }

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