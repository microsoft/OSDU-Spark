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

import com.microsoft.osdu.client.api.SchemaApi
import com.microsoft.osdu.client.invoker.ApiClient
import com.microsoft.osdu.client.model.SchemaInfo
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util

import scala.collection.mutable.ArrayBuffer
import collection.JavaConversions._

class OSDUCatalog extends TableCatalog with SupportsNamespaces {
  val logger  = LoggerFactory.getLogger(classOf[OSDUCatalog])

  private var catalogName: String = null

  private var osduOptions: OSDUOptions = _
  private var schemas: Array[Identifier] = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    catalogName = name

    osduOptions = new OSDUOptions(options)

    // setup REST client
    val client = new ApiClient()

    client.setBasePath(osduOptions.osduApiEndpoint)
    client.setBearerToken(osduOptions.getBearerToken())

    val schemaApi = new SchemaApi(client)

    // String dataPartitionId, String authority, String source, String entityType,
    // String schemaVersionMajor, String schemaVersionMinor, String status, String scope, Boolean latestVersion, Integer limit, Integer offset

    val infoFirst = schemaApi.searchSchemaInfoRepository(
      osduOptions.partitionId,
      null, null, null, null, null, null, null,
      false, 10, 0)

    val totalCount = infoFirst.getTotalCount
    var offset = infoFirst.getCount

    val schemaList = ArrayBuffer.empty[SchemaInfo]
    schemaList ++= infoFirst.getSchemaInfos

    while (schemaList.length < totalCount) {
      val infoNext = schemaApi.searchSchemaInfoRepository(
        osduOptions.partitionId,
        null, null, null, null, null, null, null,
        false, 1000, offset)

      schemaList ++= infoNext.getSchemaInfos

      offset += infoNext.getCount
    }

    schemas = schemaList.map {
      si => {
        val options = new CaseInsensitiveStringMap(scala.collection.Map(
          "osduApiEndpoint" -> osduOptions.osduApiEndpoint,
          "partitionId" -> osduOptions.partitionId,
          // TODO: pass clientId/clientSecret/oauth if supplied
          "bearerToken" -> osduOptions.getBearerToken,
          "kind" -> si.getSchemaIdentity.getId
        ))

        new OSDUIdentifier(options)
      }
    }.toArray
  }

  override def name(): String = catalogName

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    schemas
  }

  override def loadTable(ident: Identifier): Table = {
    val identMap = new java.util.HashMap[String, String]
    identMap.put("osduApiEndpoint", osduOptions.osduApiEndpoint)
    identMap.put("partitionId", osduOptions.partitionId)
    identMap.put("bearerToken", osduOptions.getBearerToken)

    val osduNamespace = ident.namespace.mkString(":")

    // TODO: not sure how to encode schema version
    identMap.put("kind", s"$osduNamespace:${ident.name()}")

    new OSDUTable(null,
      Array.empty[Transform],
      "*",
      new OSDUOptions(identMap))
  }

  // TODO
  override def createTable(ident: Identifier, schema: StructType, partitions: Array[Transform], properties: util.Map[String, String]): Table = throw new UnsupportedOperationException("Not supported")

  override def alterTable(ident: Identifier, changes: TableChange*): Table = throw new UnsupportedOperationException("Not supported")

  override def dropTable(ident: Identifier): Boolean = throw new UnsupportedOperationException("Not supported")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = throw new UnsupportedOperationException("Not supported")

  override def listNamespaces(): Array[Array[String]] = {
    Array(Array("osdu-ns"))
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    Array(Array("osdu-ns"))
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    new util.HashMap[String, String]()
  }

  override def createNamespace(namespace: Array[String], metadata: util.Map[String, String]): Unit = throw new UnsupportedOperationException("Not supported")

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = throw new UnsupportedOperationException("Not supported")

  override def dropNamespace(namespace: Array[String]): Boolean = throw new UnsupportedOperationException("Not supported")
}
