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
import org.apache.spark.sql.types.{DataTypes, StringType, StructType}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import java.util.UUID

import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}


@SerialVersionUID(1L)
class OSDUDataSourceReader(kind: String, query: String, options: DataSourceOptions)
  extends DataSourceReader with Serializable { // with SupportsPushDownRequiredColumns with SupportsPushDownFilters {
  private val logger = Logger.getLogger(classOf[OSDUDataSourceReader])

  private val defaultMaxPartitions = 1

//   var filters = Array.empty[Filter]

//   override def pruneColumns(requiredSchema: StructType): Unit = {
//       this.requiredSchema = requiredSchema
//   }

  // TODO: if the kind is specified we could
  // - fetch the OSDU schema
  // - translate to Spark schema
  def readSchema: StructType = new StructType()
    .add("JSON", StringType, true)

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

  def planInputPartitions: java.util.List[InputPartition[InternalRow]] = {
    val tableName = options.tableName.get
    // val maxPartitions = options.getInt("maxPartitions", defaultMaxPartitions)
   
    // TODO: perform first call to get the total number and then split the rest and create partitions

    new java.util.ArrayList[InputPartition[InternalRow]](Seq(new PartitionReaderFactory(query)).asJava)
  }
}

class PartitionReaderFactory(query: String) // TODO: add range
  extends InputPartition[InternalRow] {

  def createPartitionReader: InputPartitionReader[InternalRow] = {

    Logger.getLogger(classOf[OSDUDataSourceReader]).info(s"Partition reader for $query")

    new OSDUInputPartitionReader(query)
  }
}