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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}

import org.scalatest.funsuite.AnyFunSuite
import java.net.{HttpURLConnection, URL, URLEncoder}
import com.fasterxml.jackson.databind.ObjectMapper

class VerifySourceReader extends AnyFunSuite {

  val tenantId = ""
  val clientId = ""
  val clientSecret = ""
  val partitionId = ""
  val osduApiEndpoint = ""

  val oauthEndpoint = s"https://login.microsoftonline.com/$tenantId/oauth2/token"

  // val tenantId = ""
  // val clientId = ""
  // val clientSecret = ""

  // val partitionId = ""
  // val osduApiEndpoint = "https://"

  def getBearerToken(): String = { 
    val clientSecretEncoded = URLEncoder.encode(clientSecret,"UTF-8")

    val postBody = s"grant_type=client_credentials&client_id=$clientId&client_secret=$clientSecretEncoded&resource=$clientId"
    val conn = new URL(oauthEndpoint).openConnection.asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded")
    conn.setDoOutput(true)
    conn.getOutputStream().write(postBody.getBytes)
    conn.connect

    val responseJson = scala.io.Source.fromInputStream(conn.getInputStream).mkString
    val response = new ObjectMapper().readValue[java.util.Map[String, String]](responseJson, classOf[java.util.Map[String, String]])

    response.get("access_token")
  }

  test("OSDU Catalog") {
    val conf = new SparkConf()
      .setMaster("local") // local instance
      .setAppName("OSDUIntegrationTest")
      .set("spark.sql.catalog.osdu1", "com.microsoft.spark.osdu.OSDUCatalog")
      .set("spark.sql.catalog.osdu1.osduApiEndpoint", osduApiEndpoint)
      .set("spark.sql.catalog.osdu1.partitionId", partitionId)
      .set("spark.sql.catalog.osdu1.bearerToken", getBearerToken())

    val sc = SparkSession.builder().config(conf).getOrCreate()

//    sc.conf.
//    sc.conf.set
//    sc.conf.set("spark.sql.catalog.osdu.spark.cosmos.accountKey", cosmosMasterKey)
//    println(sc.sessionState.catalogManager.isCatalogRegistered("osdu"))
//    sc.sessionState.catalogManager.setCurrentCatalog("osdu")
//
//    println(sc.sessionState.catalogManager.currentCatalog.name())
//    println(sc.sessionState.catalogManager.currentCatalog.getClass)
//    println("namespace")
//    println(sc.sessionState.catalogManager.currentCatalog.defaultNamespace().mkString(":"))
//    sc.catalog.setCurrentDatabase("foooobar")
//
//    println(s"Current database: ${sc.catalog.currentDatabase}")
//
//    val tables = sc.catalog.listTables()
//    tables.show()
//
//    sc.table("foobar").show()
//    sc.sql("SELECT * FROM `osdu.db.master-data--GeoPoliticalEntity`").show()
    // osdu:wks:master-data--GeoPoliticalEntity

    // sc.sql("SHOW TABLES FROM osdu").show()
    sc.sql("SHOW TABLES FROM osdu1.osdu.wks").show()

    val df = sc.sql("SELECT * FROM osdu1.osdu.wks.`master-data--GeoPoliticalEntity:1.0.0`")

    df.printSchema()

    df.show()
  }

  test("OSDU to Spark") {
    val conf = new SparkConf()
        .setMaster("local") // local instance
        .setAppName("OSDUIntegrationTest")

    val sc = SparkSession.builder().config(conf).getOrCreate()

    val sampleDf = sc.read
        .format("com.microsoft.spark.osdu")
        .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
        .option("query", "")
        .option("osduApiEndpoint", osduApiEndpoint)
        .option("partitionId", partitionId)
        // .option("bearerToken", getBearerToken)
        .option("clientId", clientId)
        .option("clientSecret", clientSecret)
        .option("oauthEndpoint", oauthEndpoint)
        .load
        // .select("id", "kind", "data.GeoPoliticalEntityID")


    sampleDf.printSchema()

    sampleDf.show()

    sampleDf.select("id", "kind", "data.GeoPoliticalEntityID").show()
  }

  test("Spark to ODSU") {
    // TODO: the following tag needs to be pre-populated w/ the OSDU instance
    /*

POST {{LEGAL_HOST}}/legaltags
Authorization: Bearer {{access_token}}
Content-Type: application/json
data-partition-id: {{DATA_PARTITION}}

{
  "name": "{{tag}}",
  "description": "This tag is used by Check Scripts",
  "properties": {
    "countryOfOrigin": [
      "US"
    ],
    "contractId": "A1234",
    "expirationDate": "2022-12-31",
    "originator": "MyCompany",
    "dataType": "Transferred Data",
    "securityClassification": "Public",
    "personalData": "No Personal Data",
    "exportClassification": "EAR99"
  }
}

    */

    val conf = new SparkConf()
        .setMaster("local") // local instance
        .setAppName("OSDUIntegrationTest")

    val sc = SparkSession.builder().config(conf).getOrCreate()

    import sc.implicits._

    val schema = StructType(
      StructField("kind", DataTypes.StringType, false) ::
      StructField(
        "acl",
        StructType(
          StructField("viewers", ArrayType(DataTypes.StringType, false)) ::
          StructField("owners",  ArrayType(DataTypes.StringType, false)) :: Nil
        ),
        false) ::
      StructField(
        "legal",
        StructType(
          StructField("legaltags", ArrayType(DataTypes.StringType, false)) ::
          StructField("otherRelevantDataCountries",  ArrayType(DataTypes.StringType, false)) :: Nil
          // TODO: status
        ),
        false) ::
      StructField(
        "data", 
        StructType(
          StructField("GeoPoliticalEntityName", DataTypes.StringType, false) ::
          StructField("GeoPoliticalEntityID", DataTypes.StringType, false) ::
          StructField("NameAliases", ArrayType(DataTypes.StringType, false), false) :: Nil
        ),
        false) :: Nil
    )

    // println(schema)

    val rows = Seq(Row(
      // kind
      "osdu:wks:master-data--GeoPoliticalEntity:1.0.0", 
       // ACL
       Row(Seq(s"data.default.viewers@${partitionId}.contoso.com"), 
           Seq(s"data.default.owners@${partitionId}.contoso.com")),
       // Legal 
       Row(Seq(s"${partitionId}-public-usa-check-1"),
           Seq("US")),
       // Data
       Row("Austria", "AustriaID", Seq("Österreich", "Autriche"))
    ))

    val df = sc.createDataFrame(sc.sparkContext.parallelize(rows), schema)

    df.show(false)

    df.write
      .format("com.microsoft.spark.osdu")
      .mode("append")
      .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
      .option("osduApiEndpoint", osduApiEndpoint)
      .option("partitionId", partitionId)
      .option("bearerToken", getBearerToken)
      .save()
  }
}