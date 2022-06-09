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
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.net.{HttpURLConnection, URL, URLEncoder}
import com.fasterxml.jackson.databind.ObjectMapper

import java.nio.charset.StandardCharsets

class VerifySourceReader extends AnyFunSuite {

  val tenantId = "72f988bf-86f1-41af-91ab-2d7cd011db47"
  val clientId = "1cb4a4f5-d632-410e-a5e2-6134d865fdc0"
  val clientSecret = "kwR8v5JM7ExBMZo4U54eD.~2H2HZ62QKWw"
  val partitionId = "platform10731-opendes"
  val osduApiEndpoint = "https://platform10731.energy.azure.com"
  val oauthEndpoint = s"https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"

  def getBearerToken(): Unit = {

    val url = new URL("https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/v2.0/token")
    val http = url.openConnection.asInstanceOf[HttpURLConnection]
    http.setRequestMethod("POST")
    http.setDoOutput(true)
    http.setRequestProperty("content-type", "application/x-www-form-urlencoded")
    http.setRequestProperty("origin", "https://experiencelab10731.azurewebsites.net")
    http.setRequestProperty("user-agent", "vscode-restclient")

    val data = "grant_type=refresh_token&client_id=1cb4a4f5-d632-410e-a5e2-6134d865fdc0&refresh_token=0.ARoAv4j5cvGGr0GRqy180BHbR_WktBwy1g5BpeJhNNhl_cAaAK0.AgABAAEAAAD--DLA3VO7QrddgJg7WevrAgDs_wQA9P-rqLGI8oKlUALOjkLShUIJZIdbVUjRtjeaZ_LCJoyCwkSfTFGzGvCRg0b99MOitJ7vaIAcn9Yus0TUSWTSnRd8kF-PRo3pTf0xRpQVlINnh-rsX0MhBn--nVJCeNiPs8xOhJu-KNnLNJT8XIxI1mhPfynUdJ3ywFnpJeQwJQktrRi_EsRvRcaW8Aq4NOO8tyvrLxInLIrVck4UPyxgPvuU7y2FANQKxEgy_65KqtvsSW4hONJFGutAtgjnaYF6UVhHYedCeQD62VD2t762jd_S1qtu-fbs7fmzSkt3MIDKcPi_KhyLTdE4xVVKr6qG1xmxbh877ndi_Swi74LDDjjnjEea6-DQro4_YfKVofuhDEYQCoOnLWnOERtgIix23z3XDim7NpAlNQZed_2bTTcEe8POaKpZmYeoNqQxQvtpGdSUJ_dPyceZZDDL_Fsm44PlDX5V49feGM_y8BvFzuRD3Aj0Hu5e_7a0-XOU_0lxF1b-R6HPXsbQTLKbvqc4uP-P7uF_g2ZVHKqW7yAzz19ntRDAKqe8ttqvsp-J7ikE_hUtAN_rH-mQHNkITqfMURpuQNGK5sOtCVxrT-PqOcgZlm5EH8Vu6OpL0zTcg5D0iluleXWAvwMHkarwdgm8eE8L741NhFspG24b2cZjbLtH4geNf3LGzMEUhlGbR5gT8c3vdQ5UZ-NHZ_POj5XakLvtu5Y-PdiWlRFVcXFI9Tz77qXzkyuIpq5VQ6Vc_hqSYhvsJEHZdYkQp0mfkx0LZo14g_eESipIkjIMP-EcYPKLeMO8y7sJ7ZNRy-G1JOG9mNGdCnlEtIt7QfX1Xcbdh2RMRoYLAAzrcJ611ZCMfCLQG1G2Ifx9ou3DfXL2vW2K9DxTQqfk0YjZSAZq8gKd-bdSMsk1qXjSM6fJQ2xmnejtK7kw4PNqqGr0qQHv8Qa1f0s4rJw6tjByCIifTs2mjH7WTwHf7KMvZ2GxIB1D4tvshZOgiRcivyt53YBWhAHg2kIhq18pLPLfxfTxEA&scope=1cb4a4f5-d632-410e-a5e2-6134d865fdc0/.default openid profile offline_access"

    val out: Array[Byte] = data.getBytes(StandardCharsets.UTF_8)

    val stream = http.getOutputStream
    stream.write(out)

    System.out.println(http.getResponseCode + " " + http.getResponseMessage)
  }

//   test("OSDU Catalog") {
//     val conf = new SparkConf()
//       .setMaster("local") // local instance
//       .setAppName("OSDUIntegrationTest")
//       .set("spark.sql.catalog.osdu1", "com.microsoft.spark.osdu.OSDUCatalog")
//       .set("spark.sql.catalog.osdu1.osduApiEndpoint", osduApiEndpoint)
//       .set("spark.sql.catalog.osdu1.partitionId", partitionId)
//       .set("spark.sql.catalog.osdu1.bearerToken", getBearerToken())

//     val sc = SparkSession.builder().config(conf).getOrCreate()

// //    sc.conf.
// //    sc.conf.set
// //    sc.conf.set("spark.sql.catalog.osdu.spark.cosmos.accountKey", cosmosMasterKey)
// //    println(sc.sessionState.catalogManager.isCatalogRegistered("osdu"))
// //    sc.sessionState.catalogManager.setCurrentCatalog("osdu")
// //
// //    println(sc.sessionState.catalogManager.currentCatalog.name())
// //    println(sc.sessionState.catalogManager.currentCatalog.getClass)
// //    println("namespace")
// //    println(sc.sessionState.catalogManager.currentCatalog.defaultNamespace().mkString(":"))
// //    sc.catalog.setCurrentDatabase("foooobar")
// //
// //    println(s"Current database: ${sc.catalog.currentDatabase}")
// //
// //    val tables = sc.catalog.listTables()
// //    tables.show()
// //
// //    sc.table("foobar").show()
// //    sc.sql("SELECT * FROM `osdu.db.master-data--GeoPoliticalEntity`").show()
//     // osdu:wks:master-data--GeoPoliticalEntity

//     // sc.sql("SHOW TABLES FROM osdu").show()
//     sc.sql("SHOW TABLES FROM osdu1.osdu.wks").show()

//     val df = sc.sql("SELECT * FROM osdu1.osdu.wks.`master-data--GeoPoliticalEntity:1.0.0`")

//     df.printSchema()

//     df.show()
//   }

   test("OSDU to Spark") {
     val conf = new SparkConf()
         .setMaster("local") // local instance
         .setAppName("OSDUIntegrationTest")

     val sc = SparkSession.builder().config(conf).getOrCreate()
     val sampleDf = sc.read
         .format("com.microsoft.spark.osdu")
         .option("kind", "osdu:wks:reference-data--ProcessingParameterType:1.0.0")
         .option("query", "")
         .option("osduApiEndpoint", osduApiEndpoint)
         .option("partitionId", partitionId)
         .option("clientId", clientId)
         .option("clientSecret", clientSecret)
         .option("oauthEndpoint", oauthEndpoint)
         .load()
         .select("data.ID", "data.Name", "data.Source")

     sampleDf.printSchema()
     sampleDf.show(false)
     //sampleDf.select("data.ID").show(1000,false)
   }

 test("Spark to OSDU") {
//     // TODO: the following tag needs to be pre-populated w/ the OSDU instance
//

   /* POST {{LEGAL_HOST}}/legaltags
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
           StructField("Name", DataTypes.StringType, false) ::
           StructField("ID", DataTypes.StringType, false) ::
           StructField("Code", DataTypes.StringType, false) ::
           StructField("Source", DataTypes.StringType, false):: Nil

         ),
         false) :: Nil
     )

     // println(schema)

     val rows = Seq(Row(
       // kind
       "osdu:wks:reference-data--ProcessingParameterType:1.0.0",
        // ACL
        Row(Seq("data.default.viewers@platform10731-opendes.contoso.com"),
            Seq("data.default.owners@platform10731-opendes.contoso.com")),
        // Legal
        Row(Seq("platform10731-opendes-public-usa-check-1-test-spancholi"),
            Seq("US")),
        // Data
        Row("QA Test Case11 - spancholi", "qatest11", "QA Test Case 11", "spark-to-osdu-load-testcase11"))
     )

     val df = sc.createDataFrame(sc.sparkContext.parallelize(rows), schema)

     df.show(false)

     df.write
       .format("com.microsoft.spark.osdu")
       .mode("append")
       .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
       .option("osduApiEndpoint", osduApiEndpoint)
       .option("partitionId", partitionId)
       .option("clientId", clientId)
       .option("clientSecret", clientSecret)
       .option("oauthEndpoint", oauthEndpoint)
       .save()
   }
}