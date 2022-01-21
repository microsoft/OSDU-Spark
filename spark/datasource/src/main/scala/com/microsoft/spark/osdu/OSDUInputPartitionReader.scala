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

import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import scala.collection.JavaConverters._

import osdu.client.ApiClient
import osdu.client.api.SearchApi

@SerialVersionUID(1L)
class OSDUInputPartitionReader(query: String)
  extends InputPartitionReader[InternalRow] with Serializable {

  private val logger = Logger.getLogger(classOf[OSDUInputPartitionReader])

  private var currentRow: InternalRow = _

  private val client = new ApiClient()

  override def close(): Unit = {
    // if (scanner != null)
    //   scanner.close()

    // if (client != null)
    //   client.close()
  }

  @IOException
  override def next: Boolean = {
      return false
    // if (scannerIterator.hasNext) {
    //   val entry = scannerIterator.next
    //   val data = entry.getValue.get

    //   // byte[] -> avro
    //   decoder = DecoderFactory.get.binaryDecoder(data, decoder)
    //   datum = reader.read(datum, decoder)

    //   // avro -> catalyst
    //   currentRow = deserializer.deserialize(datum).asInstanceOf[InternalRow]

    //   if (rowKeyColumnIndex >= 0) {
    //     // move row key id into internalrow
    //     entry.getKey.getRow(rowKeyText)

    //     // avoid yet another byte array copy...
    //     val str = UTF8String.fromBytes(rowKeyText.getBytes, 0, rowKeyText.getLength)
    //     currentRow.update(rowKeyColumnIndex, str)
    //   }

    //   true
    // } else {
    //   false
    // }
  }

  override def get(): InternalRow = currentRow
}