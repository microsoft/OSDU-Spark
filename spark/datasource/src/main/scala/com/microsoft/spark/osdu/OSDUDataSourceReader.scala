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
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StringType, BooleanType, IntegerType, StructField, StructType}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import java.util.{ArrayList, Map, List, UUID}

import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}

import osdu.client.{ApiClient, Configuration}
import osdu.client.api.SchemaApi

@SerialVersionUID(1L)
class OSDUDataSourceReader(options: DataSourceOptions)
  extends DataSourceReader with Serializable with SupportsPushDownRequiredColumns { // with SupportsPushDownFilters {
  private val logger = Logger.getLogger(classOf[OSDUDataSourceReader])

  private val defaultMaxPartitions = 1

  // get input parameters
  private val kind = options.get("kind").orElse("")
  private val query = options.get("query").orElse("")
  private val oakApiEndpoint = options.get("oakApiEndpoint").get
  private val partitionId = options.get("partitionId").get
  private val bearerToken = options.get("bearerToken").get

  var prunedSchema: Option[StructType] = None

  /** Prune the schema by removing all fields that are not specified by the user.
   */ 
  override def pruneColumns(requiredSchema: StructType): Unit = {
    //  println(s"Discovered Schema $schemaForKind")
    //  println(s"RequiredSchema    $requiredSchema")
     prunedSchema = Some(requiredSchema)
  }

  // TODO: why is this executed twice???
  private lazy val schemaForKind = {
    // setup REST client
    val client = new ApiClient()

    client.setBasePath(oakApiEndpoint)
    client.setApiKey(bearerToken)
    client.setApiKeyPrefix("Bearer")
    client.addDefaultHeader("data-partition-id", partitionId)

    val schemaApi = new SchemaApi(client)

    // fetch OSDU schema from service
    val schema = schemaApi.getSchema(partitionId, kind).asInstanceOf[Map[String, Object]]

    // definitions are top-level
    val definitions = schema.get("definitions").asInstanceOf[Map[String, Map[String, Object]]]

    // convert OSDU schema to Spark SQL schema
    new OSDUSchemaConverter(definitions).osduSchemaToStruct(schema).get
  }

  // The final schema used to read the data
  def readSchema: StructType = prunedSchema.getOrElse(schemaForKind)

  // TODO: implement pushFilters.
//   override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//     // unfortunately predicates on nested elements are not pushed down by Spark
//     // https://issues.apache.org/jira/browse/SPARK-17636
//     // https://github.com/apache/spark/pull/22535

//     val jsonSchema = AvroUtil.catalystSchemaToJson(schemaWithOutRowKey)
//     val result = new FilterToJuel(jsonSchema.attributeToVariableMapping, rowKeyColumn)
//       .serializeFilters(filters, options.get("filter").orElse(""))

//     this.filters = result.supportedFilters.toArray

//     if (result.serializedFilter.length > 0) {
//       this.filterInJuel = Some("${" + result.serializedFilter + "}")
//       logger.info(s"JUEL filter: ${this.filterInJuel}")
//     }

//     result.unsupportedFilters.toArray
//   }

//   override def pushedFilters(): Array[Filter] = filters

  def planInputPartitions: List[InputPartition[InternalRow]] = {
    // TODO: we could return multiple readers to parallelize the invocation, but we'd loose consistency as I'm not sure how to share the cursor

    // create a single partition to read
    new ArrayList[InputPartition[InternalRow]](
      Seq(
        new PartitionReaderFactory(
          kind,
          query,
          oakApiEndpoint,
          partitionId,
          bearerToken,
          prunedSchema.getOrElse(schemaForKind))).asJava)
  }
}

class PartitionReaderFactory(
  kind: String,
  query: String,
  oakApiEndpoint: String,
  partitionId: String,
  bearerToken: String,
  schema: StructType
  )
  extends InputPartition[InternalRow] {

  def createPartitionReader: InputPartitionReader[InternalRow] = {

    Logger.getLogger(classOf[OSDUDataSourceReader]).info(s"Partition reader for $kind & $query")

    new OSDUInputPartitionReader(kind, query, oakApiEndpoint, partitionId, bearerToken, schema)
  }
}