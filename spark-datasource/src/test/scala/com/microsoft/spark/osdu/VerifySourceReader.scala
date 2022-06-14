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

import java.util

class VerifySourceReader extends AnyFunSuite {


  /*def getBearerToken(): String = {


    val map = new util.HashMap[String, String]()
    map.put("clientSecret", clientSecret)
    map.put("clientId", clientId)
    map.put("oauthEndpoint", oauthEndpoint)

    return AuthUtil.getBearerToken(map)
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
    sampleDf.show(50,false)
  }

  test("Spark to OSDU") {
    //     // TODO: the following tag needs to be pre-populated w/ the OSDU instance
    val conf = new SparkConf()
      .setMaster("local") // local instance
      .setAppName("OSDUIntegrationTest")

    val sc = SparkSession.builder().config(conf).getOrCreate()

    val schema = StructType(
      StructField("kind", DataTypes.StringType, false) ::
        StructField(
          "acl",
          StructType(
            StructField("viewers", ArrayType(DataTypes.StringType, false)) ::
              StructField("owners", ArrayType(DataTypes.StringType, false)) :: Nil
          ),
          false) ::
        StructField(
          "legal",
          StructType(
            StructField("legaltags", ArrayType(DataTypes.StringType, false)) ::
              StructField("otherRelevantDataCountries", ArrayType(DataTypes.StringType, false)) :: Nil
            // TODO: status
          ),
          false) ::
        StructField(
          "data",
          StructType(
            StructField("Name", DataTypes.StringType, false) ::
              StructField("ID", DataTypes.StringType, false) ::
              StructField("Code", DataTypes.StringType, false) ::
              StructField("Source", DataTypes.StringType, false) :: Nil

          ),
          false) :: Nil
    )

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
      Row("QA Test Case16 - spancholi", "qatest16", "QA Test Case 16", "spark-to-osdu-load-testcase16"))
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
  }*/
}