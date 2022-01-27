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
        .option("oakApiEndpoint", "https://oakkmtest.oep.ppe.azure-int.net")
        .option("partitionId", "oakkmtest-opendes")
        .option("bearerToken", "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Ik1yNS1BVWliZkJpaTdOZDFqQmViYXhib1hXMCIsImtpZCI6Ik1yNS1BVWliZkJpaTdOZDFqQmViYXhib1hXMCJ9.eyJhdWQiOiIyZjU5YWJiYy03YjQwLTRkMGUtOTFiMi0yMmNhMzA4NGJjODQiLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWF0IjoxNjQzMjMzMTM0LCJuYmYiOjE2NDMyMzMxMzQsImV4cCI6MTY0MzMxOTgzNCwiYWlvIjoiRTJaZ1lCRDhZOTYxZUxlL25INm5qZVViMFU5K0FBPT0iLCJhcHBpZCI6IjJmNTlhYmJjLTdiNDAtNGQwZS05MWIyLTIyY2EzMDg0YmM4NCIsImFwcGlkYWNyIjoiMSIsImlkcCI6Imh0dHBzOi8vc3RzLndpbmRvd3MubmV0LzcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0Ny8iLCJvaWQiOiI1MWQyZjc5MS03OTViLTRjOGQtOTY1Ny1jZDIzYjFmOWYyYTciLCJyaCI6IjAuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUjd5cldTOUFldzVOa2JJaXlqQ0V2SVFhQUFBLiIsInJvbGVzIjpbIkF6dXJlRXZlbnRHcmlkU2VjdXJlV2ViaG9va1N1YnNjcmliZXIiXSwic3ViIjoiNTFkMmY3OTEtNzk1Yi00YzhkLTk2NTctY2QyM2IxZjlmMmE3IiwidGlkIjoiNzJmOTg4YmYtODZmMS00MWFmLTkxYWItMmQ3Y2QwMTFkYjQ3IiwidXRpIjoidjJVdklOZWViMGVDVXVCUDREZy1BQSIsInZlciI6IjEuMCJ9.dHJKD1dtZXFgILS-xACdvZxu5c8QwE3mAq4aDiqv6G0h8xO_4R1CUwZfIXF3jYnuEeSZKW0nliFkXB5UtofZALBGOUNV40jo1_MdFQxw2ZjtEUQ-L045FiBNnP3E9n-gPYnmrAztozlUnCP9v9Akie-y-2tnpWwDv6COpnBeSEd3vthWbMeMTM5Q5MDiIcm1BOAMpUCXUWKBAD_WBruSgrrJVHvofbkyUxuASdzINIuUGWVhDC5CPNAs3wF2gSN00CTOftgMAt5lHfN9eVt5Ez980UW8uqb5wFsGpBfsvp_kYlDFCXQ5wTxj_xum9wnw3cpU_CJqi-nU5Qpz2j5ufA")
        .load
        // .select("id", "kind", "data.GeoPoliticalEntityID")

    sampleDf.printSchema()

    sampleDf.show()

    sampleDf.select("id", "kind", "data.GeoPoliticalEntityID").show()
  }
}