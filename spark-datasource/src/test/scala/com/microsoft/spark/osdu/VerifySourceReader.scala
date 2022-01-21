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

class VerifySourceReader extends AnyFunSuite {
  test("OSDU to Spark") {
    // val conf = new SparkConf()
    //     .setMaster("local") // local instance
    //     .setAppName("OSDUIntegrationTest")

    // val sc = SparkSession.builder().config(conf).getOrCreate()

    // val sampleDf = sc.read
    //     .format("com.microsoft.spark.osdu")
    //     .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
    //     .option("query", "")
    //     .option("osduApiEndpoint", "https://YYY.energy.azure.com")
    //     .option("partitionId", "YYY-opendes")
    //     .option("bearerToken", "XXX")
    //     .load
    //     // .select("id", "kind", "data.GeoPoliticalEntityID")


    // sampleDf.printSchema()

    // sampleDf.show()

    // sampleDf.select("id", "kind", "data.GeoPoliticalEntityID").show()
  }

  test("Spark to ODSU") {
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
          StructField("GeoPoliticalEntityID", DataTypes.StringType, false) ::
          StructField("GeoPoliticalEntityName", DataTypes.StringType, false) ::
          StructField("NameAliases", ArrayType(DataTypes.StringType, false), false) :: Nil
        ),
        false) :: Nil
    )

    // println(schema)

    val rows = Seq(Row(
      // kind
      "osdu:wks:master-data--GeoPoliticalEntity:1.0.0", 
       // ACL
       Row(Seq("data.default.viewers@projectosdu5543-opendes.contoso.com"), 
           Seq("data.default.owners@projectosdu5543-opendes.contoso.com")),
       // Legal 
       Row(Seq("projectosdu5543-opendes-public-usa-check-1"),
           Seq("US")),
       // Data
       Row("AustriaID", "Austria", Seq("Österreich", "Autriche"))
    ))

    val df = sc.createDataFrame(sc.sparkContext.parallelize(rows), schema)

    df.show(false)

    df.write
      .format("com.microsoft.spark.osdu")
      .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
      .option("osduApiEndpoint", "https://XXX.energy.azure.com")
      .option("partitionId", "projectosdu5543-opendes")
      .option("bearerToken", "YYY")
      .save()
  }
}