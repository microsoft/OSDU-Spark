package com.microsoft.spark.osdu

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.microsoft.osdu.client.model.StorageRecord
import scala.collection.mutable.MutableList
import org.asynchttpclient.DefaultAsyncHttpClient

import scala.collection.JavaConverters.seqAsJavaListConverter
import org.slf4j.LoggerFactory


class OSDUStorageClient {

  val logger = LoggerFactory.getLogger(classOf[OSDUStorageClient])

  def postRecords(osduApiEndpoint: String, partitionId: String, bearerToken: String, storageRecord: MutableList[StorageRecord]): Unit = {

    val client = new DefaultAsyncHttpClient
    val localVarPath = "/api/storage/v2/records"
    val url = osduApiEndpoint + localVarPath

    try {
      val objectMapper = new ObjectMapper()
      val json = objectMapper.writeValueAsString(storageRecord.asJava)

      val response = client.prepare("PUT", url)
        .setHeader("user-agent", "Spark")
        .setHeader("authorization", "Bearer " + bearerToken)
        .setHeader("content-type", "application/json")
        .setHeader("data-partition-id", partitionId)
        .setBody(json)
        .execute
        .toCompletableFuture
        .join()

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
