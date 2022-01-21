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

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData

import scala.collection.JavaConverters._

import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite

class VerifyOSDURecordConverter extends AnyFunSuite {
  test("Primitive Types") {
    val schema = StructType(
        StructField("a", DataTypes.IntegerType) ::
        StructField("b", DataTypes.DoubleType) ::
        StructField("c", DataTypes.StringType) :: Nil)

    val data =  Map("a" -> 1.0, // Note: the service returns a double literal even though it's an integer
                    "b" -> 2.3,
                    "c" -> "str")
                    .asJava
                    .asInstanceOf[java.util.Map[String, Object]]

    val actual= new OSDURecordConverter(schema).toInternalRow(data)
    val expected = InternalRow.fromSeq(Seq(1, 2.3, "str"))

    // println(row.get(0, DataTypes.StringType).getClass)
    // println(row.get(1, DataTypes.StringType).getClass)
    // println(row.get(2, DataTypes.StringType).getClass)

    // println(expected.get(0, DataTypes.StringType).getClass)
    // println(expected.get(1, DataTypes.StringType).getClass)
    // println(expected.get(2, DataTypes.StringType).getClass)

    assert(actual == expected)
  }

  test("Primitive Types Reverse (keep integer)") {
    val schema = StructType(
        StructField("a", DataTypes.IntegerType) ::
        StructField("b", DataTypes.DoubleType) ::
        StructField("c", DataTypes.StringType) :: Nil)

    val expected =  Map("a" -> 1,
                        "b" -> 2.3,
                        "c" -> "str")
                        .asJava
                        .asInstanceOf[java.util.Map[String, Object]]

    val row = InternalRow.fromSeq(Seq(1, 2.3, "str"))
    val actual = new OSDURecordConverter(schema).toJava(row)

    assert(actual == expected)
  }

  test("Nested Types") {
    val schema = StructType(
        StructField("a", 
          StructType(
            StructField("x", DataTypes.DoubleType) ::
            StructField("y", DataTypes.DoubleType) :: Nil)) ::
        StructField("c", DataTypes.StringType) :: Nil)

    val data =  Map("a" -> Map("x" -> 1.0, "y" -> 2.0).asJava,
                    "c" -> "str")
                    .asJava
                    .asInstanceOf[java.util.Map[String, Object]]

    val converter = new OSDURecordConverter(schema)
    val row = InternalRow.fromSeq(Seq(InternalRow.fromSeq(Seq(1.0, 2.0)), "str"))

    val actualRow = converter.toInternalRow(data)
    val actualData = converter.toJava(row)

    assert(actualRow == row)
    assert(actualData == data)
  }

  test("Array") {
    val schema = StructType(StructField("a", ArrayType(DataTypes.StringType)) :: Nil)

    val data =  Map("a" -> Seq("x", "y").asJava)
                    .asJava
                    .asInstanceOf[java.util.Map[String, Object]]

    val converter = new OSDURecordConverter(schema)
    val row = InternalRow.fromSeq(Seq(ArrayData.toArrayData(Seq("x", "y"))))

    val actualRow = converter.toInternalRow(data)
    val actualData = converter.toJava(row)

    assert(actualRow == row)
    assert(actualData == data)
  }

  // TODO: empty array
  // TODO: array w/ int/double/...
  // TODO: array w/ struct
}