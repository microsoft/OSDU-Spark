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

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite

class VerifyOSDUSchemaConverter extends AnyFunSuite {
  test("Primitive types") {
    // String to JSON

    // invoke osduSchemaToStruct

    // validate
  }

  test("Schema to dot-paths | Flat") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", DataTypes.IntegerType) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a", "b")
  }

  test("Schema to dot-paths | Nested") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", 
          StructType(
            StructField("x", DataTypes.IntegerType) ::
            StructField("y", DataTypes.IntegerType) :: Nil)) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a.x", "a.y", "b")
  }

   test("Schema to dot-paths | Double nested") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", 
          StructType(
            StructField("x", DataTypes.IntegerType) ::
            StructField("y", StructType(StructField("z", DataTypes.IntegerType) :: Nil)) :: Nil)) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a.x", "a.y.z", "b")
  }

  test("Schema to dot-paths | Array ") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(StructField("a", ArrayType(DataTypes.StringType)) :: Nil))

    paths should contain ("a")
  }

  test("Schema to dot-paths | Array nested ") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a",
          ArrayType(
            StructType(
              StructField("x", DataTypes.StringType) ::
              StructField("y", DataTypes.StringType) :: Nil))) :: Nil))

    paths should contain inOrder ("a.x", "a.y")
  }
}