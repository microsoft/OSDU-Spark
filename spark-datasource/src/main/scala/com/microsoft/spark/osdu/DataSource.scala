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

import org.apache.spark.sql.SparkSession

import java.util
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, SupportsCatalogOptions, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform

import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Spark Table provider implementation for OSDU Record API */
class DefaultSource extends TableProvider with SupportsCatalogOptions {

  // override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType =
    getTable(null, Array.empty[Transform], caseInsensitiveStringMap.asCaseSensitiveMap()).schema()

  override def getTable(structType: StructType, transforms: Array[Transform], map: util.Map[String, String]): Table =
    new OSDUTable(structType, transforms, "*", new OSDUOptions(map))

  def setupDefaultSparkCatalog(spark: SparkSession, options: CaseInsensitiveStringMap) = {
    spark.conf.set("spark.sql.catalog.osdu", "com.microsoft.spark.osdu.OSDUCatalog")
    spark.conf.set("spark.sql.catalog.osdu.kind", options.get("kind"))
    spark.conf.set("spark.sql.catalog.osdu.osduApiEndpoint", options.get("osduApiEndpoint"))
    spark.conf.set("spark.sql.catalog.osdu.partitionId", options.get("partitionId"))
    spark.conf.set("spark.sql.catalog.osdu.clientId", options.get("clientId"))

    if (options.containsKey("bearerToken"))
      spark.conf.set("spark.sql.catalog.osdu.bearerToken", options.get("bearerToken"))
    else {
      spark.conf.set("spark.sql.catalog.osdu.clientSecret", options.get("clientSecret"))
      spark.conf.set("spark.sql.catalog.osdu.oauthEndpoint", options.get("oauthEndpoint"))
    }
    // spark.sessionState.catalogManager.catalog("osdu")
  }

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val spark = SparkSession.active;
    setupDefaultSparkCatalog(spark, options);

    new OSDUIdentifier(options)
  }

  override def extractCatalog(options: CaseInsensitiveStringMap): String = {
    "osdu"
  }
}

class OSDUIdentifier(options: CaseInsensitiveStringMap) extends Identifier {

  val osduOptions = new OSDUOptions(options)

  private val kindParts = osduOptions.kind.split(':')

  // Sample: osdu:wks:master-data--GeoPoliticalEntity:1.0.0"
  override def namespace(): Array[String] = kindParts.slice(0, 2)

  override def name(): String = kindParts.slice(2, kindParts.length).mkString(":")

  val optionsAsHashMap = options
}