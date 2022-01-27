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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.v2.DataSourceOptions

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

import org.scalatest.FunSuite

class VerifySourceReader extends FunSuite {
  test("With Spark Context") {
    val options = new java.util.HashMap[String, String]()

    val conf = new SparkConf()
        .setMaster("local") // local instance
        .setAppName("OSDUIntegrationTest")

    val sc = SparkSession.builder().config(conf).getOrCreate()

    val sampleDf = sc.read
        .format("com.microsoft.spark.osdu")
        .option("kind", "osdu:wks:master-data--GeoPoliticalEntity:1.0.0")
        .option("query", "")
        .option("oakApiEndpoint", "https://<INSERT VALUE>")
        .option("partitionId", "<INSERT VALUE>")
        .option("bearerToken", "<INSERT VALUE>")
        .load
        // .select("id", "kind", "data.GeoPoliticalEntityID")

    sampleDf.printSchema()

    sampleDf.show()

    sampleDf.select("id", "kind", "data.GeoPoliticalEntityID").show()
  }
}