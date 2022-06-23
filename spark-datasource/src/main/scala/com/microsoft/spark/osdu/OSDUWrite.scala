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
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriter, DataWriterFactory, LogicalWriteInfo, PhysicalWriteInfo, Write, WriterCommitMessage}

class OSDUWrite(logicalInfo: LogicalWriteInfo, pyhsicalInfo: PhysicalWriteInfo) extends Write {
  override def toBatch(): BatchWrite = new OSDUBatchStreamWrite(logicalInfo, pyhsicalInfo)

  override def toStreaming: StreamingWrite = new OSDUBatchStreamWrite(logicalInfo, pyhsicalInfo)
}


class OSDUBatchStreamWrite(logicalInfo: LogicalWriteInfo, pyhsicalInfo: PhysicalWriteInfo) extends BatchWrite with StreamingWrite {
  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory
  = new OSDUDataWriterFactory(
    logicalInfo.options.get("osduApiEndpoint"),
    logicalInfo.options.get("partitionId"),
    AuthUtil.getBearerToken(logicalInfo.options),
    logicalInfo.schema)

  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory
  = new OSDUDataWriterFactory(
    logicalInfo.options.get("osduApiEndpoint"),
    logicalInfo.options.get("partitionId"),
    AuthUtil.getBearerToken(logicalInfo.options),
    logicalInfo.schema)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
  }

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
  }

  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {
  }
}

class OSDUDataWriterFactory(osduApiEndpoint: String, osduPartitionId: String, bearerToken: String, schema: StructType)
  extends DataWriterFactory with StreamingDataWriterFactory{

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow]
  = new OSDUDataWriter(osduApiEndpoint, osduPartitionId, bearerToken, schema)

  override def createWriter(partitionId: Int, taskId: Long, epochId: Long): DataWriter[InternalRow]
  = new OSDUDataWriter(osduApiEndpoint, osduPartitionId, bearerToken, schema)
}