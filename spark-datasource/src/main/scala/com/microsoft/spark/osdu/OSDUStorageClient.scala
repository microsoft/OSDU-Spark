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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.osdu.client.model.StorageRecord
import scala.collection.mutable.MutableList
import org.asynchttpclient.DefaultAsyncHttpClient

import scala.collection.JavaConverters.seqAsJavaListConverter
import org.slf4j.LoggerFactory

/**
 * This is temporary till mechanism to post
 * data to OSDU. Once OSDUSDK's response value
 * is fixed this will be deprecated.
 */

class OSDUStorageClient {

  val logger = LoggerFactory.getLogger(classOf[OSDUStorageClient])

  /**
   *
   * @param osduApiEndpoint - OSDU End point for posting data
   * @param partitionId - Data partition id
   * @param bearerToken - Token for OAUTH authentication
   * @param storageRecord - Data/Metadata to be stored
   */
  def postRecords(osduApiEndpoint: String, partitionId: String, bearerToken: String, storageRecord: MutableList[StorageRecord]): Unit = {

    val client = new DefaultAsyncHttpClient
    val localVarPath = "/api/storage/v2/records"
    val url = osduApiEndpoint + localVarPath

    try {
      val objectMapper = new ObjectMapper()
      val json = objectMapper.writeValueAsString(storageRecord.asJava)

      val future = client.prepare("PUT", url)
        .setHeader("user-agent", "Spark")
        .setHeader("authorization", "Bearer " + bearerToken)
        .setHeader("content-type", "application/json")
        .setHeader("data-partition-id", partitionId)
        .setBody(json)
        .execute()

      val response =future.get()

      response.getStatusCode match {
        case 201 => logger.info("Request was completed successfully. Response Message: \n" + response.getResponseBody())
        case _ => logger.warn("Write request failed. Response Message: \n" + response.getResponseBody())
      }

    } catch {
      case e: JsonProcessingException => {
        logger.error("Could not convert Storage Record to Json String")
        e.printStackTrace()
      }
    } finally {
      client.close()
    }

  }
}
