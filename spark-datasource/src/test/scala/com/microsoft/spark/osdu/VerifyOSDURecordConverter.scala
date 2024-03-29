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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite

import java.time.temporal.ChronoUnit
import java.time.{LocalDate, LocalTime, ZoneId}

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
    val expected = InternalRow.fromSeq(Seq(1, 2.3, UTF8String.fromString("str")))

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
        StructField("d", DataTypes.DoubleType) ::
        StructField("c", DataTypes.StringType) :: Nil)

    val expected =  Map("a" -> 1,
                        "b" -> 2.3,
                        "d" -> 3000000L,
                        "c" -> "str")
                        .asJava
                        .asInstanceOf[java.util.Map[String, Object]]

    val row = InternalRow.fromSeq(Seq(1, 2.3, 3000000L, "str"))
    val actual = new OSDURecordConverter(schema).toJava(row)

    assert(actual == expected)
  }

  test("Date Type") {
    val schema = StructType(
      StructField("a", DataTypes.DateType) ::
      StructField("b", DataTypes.DateType) ::
        Nil)

    val data =  Map("a" -> "2022-01-03", "b" -> null)
      .asJava
      .asInstanceOf[java.util.Map[String, Object]]

    val row = InternalRow.fromSeq(Seq(
      ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.parse("2022-01-03")).toInt,
      null))

    val converter = new OSDURecordConverter(schema)

    val actualRow = converter.toInternalRow(data)

    assert(actualRow == row)
  }

  test("Date Type Deserialize") {
    val schema = StructType(
      StructField("a", DataTypes.DateType) ::
        StructField("b", DataTypes.DateType) ::
        Nil)

    val data =  Map("a" -> "2022-01-03", "b" -> null)
      .asJava
      .asInstanceOf[java.util.Map[String, Object]]

    val dataExpected =  Map("a" -> "2022-01-03")
      .asJava
      .asInstanceOf[java.util.Map[String, Object]]

    val row = InternalRow.fromSeq(Seq(
      ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.parse("2022-01-03")).toInt,
      null))

    val converter = new OSDURecordConverter(schema)

    val actualData = converter.toJava(row)
    assert(actualData == dataExpected)
  }

  test("Map") {
    val schema = StructType(
      StructField("a", MapType(StringType, StringType, false)) :: Nil)

    val data =  Map("a" -> Map("x" -> "abc", "y" -> "def").asJava)
      .asJava
      .asInstanceOf[java.util.Map[String, Object]]

    val converter = new OSDURecordConverter(schema)
    val row = InternalRow.fromSeq(Seq(ArrayBasedMapData.apply(Map(
      UTF8String.fromString("x") -> UTF8String.fromString("abc"),
      UTF8String.fromString("y") -> UTF8String.fromString("def")
    ))))

    val actualRow = converter.toInternalRow(data)
    val actualData = converter.toJava(row)

    // assert(actualData == data)
    assert(actualRow.getMap(0).keyArray() == row.getMap(0).keyArray())
    assert(actualRow.getMap(0).valueArray() == row.getMap(0).valueArray())
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

    // assert(actualRow == row) // TODO
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

    // assert(actualRow == row) // TODO
    assert(actualData == data)
  }

  // TODO: empty array
  // TODO: array w/ int/double/...
  // TODO: array w/ struct
}