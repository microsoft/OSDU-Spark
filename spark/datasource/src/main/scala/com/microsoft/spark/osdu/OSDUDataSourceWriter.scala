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

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import org.apache.log4j.Logger

class OSDUDataSourceWriter(schema: StructType, mode: SaveMode, options: DataSourceOptions)
  extends DataSourceWriter {

  private val logger = Logger.getLogger(classOf[OSDUDataSourceWriter])

  // get input parameters
  private val oakApiEndpoint = options.get("oakApiEndpoint").get
  private val partitionId = options.get("partitionId").get
  private val bearerToken = options.get("bearerToken").get

  // TODO: validate schema
  // assume
  // - kind: string
  // - acl
  // -- owners: list[str]
  // -- viewers: list[str]}
  // - legal
  // -- legaltags: list[str]
  // -- otherRelevantDataCountries: list[str]
  // -- status: str
  // - tags: map[str, str]
  // - data: struct

  // - id: str (optional)

  override def createWriterFactory(): DataWriterFactory[InternalRow] = {
    new OSDUDataWriterFactory(oakApiEndpoint, partitionId, bearerToken, schema)
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }
}

class OSDUDataWriterFactory(
  oakApiEndpoint: String,
  oakPartitionId: String,
  bearerToken: String,
  schema: StructType
  )
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow] = {
    new OSDUDataWriter(oakApiEndpoint, oakPartitionId, bearerToken, schema)
  }
}