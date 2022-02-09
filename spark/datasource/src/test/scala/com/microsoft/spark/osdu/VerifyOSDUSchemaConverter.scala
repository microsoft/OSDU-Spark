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
import org.scalatest.matchers.should.Matchers._
import org.scalatest.funsuite.AnyFunSuite
import scala.io.Source

import java.util.Map
import osdu.client.JSON
import java.lang.reflect.Type
import com.google.gson.reflect.TypeToken

class VerifyOSDUSchemaConverterDotPath extends AnyFunSuite {
  test("Flat") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", DataTypes.IntegerType) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a", "b")
  }

  test("Nested") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", 
          StructType(
            StructField("x", DataTypes.IntegerType) ::
            StructField("y", DataTypes.IntegerType) :: Nil)) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a.x", "a.y", "b")
  }

   test("Double nested") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(
        StructField("a", 
          StructType(
            StructField("x", DataTypes.IntegerType) ::
            StructField("y", StructType(StructField("z", DataTypes.IntegerType) :: Nil)) :: Nil)) ::
        StructField("b", DataTypes.StringType) :: Nil))

    paths should contain inOrder ("a.x", "a.y.z", "b")
  }

  test("Array") {
    val paths = OSDUSchemaConverter.schemaToPaths(
      StructType(StructField("a", ArrayType(DataTypes.StringType)) :: Nil))

    paths should contain ("a")
  }

  test("Array nested ") {
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

class VerifyOSDUSchemaConverter extends AnyFunSuite {
  /** Only keep name, type and nullable */
  def stripSchema(struct: StructType): StructType = {
    StructType(struct.fields.map { 
      f => {
        val strippedDataType = 
          if (f.dataType.isInstanceOf[StructType])
            stripSchema(f.dataType.asInstanceOf[StructType])
          else if (f.dataType.isInstanceOf[ArrayType]) {
            val arrType = f.dataType.asInstanceOf[ArrayType]
            
            val elemType =
              if (arrType.elementType.isInstanceOf[StructType])
                stripSchema(arrType.elementType.asInstanceOf[StructType])
              else
                arrType.elementType

            ArrayType(elemType, arrType.containsNull)
          }
          else 
            f.dataType
          
        StructField(f.name, strippedDataType, f.nullable)
      }  
    })
  }

  def prettyPrintSchema(struct: StructType, prefix: String = ""): Unit = {
   struct.fields.map { 
      f => {
          if (f.dataType.isInstanceOf[StructType]) {
            println(s"${prefix}${f.name}")
            prettyPrintSchema(f.dataType.asInstanceOf[StructType], prefix + "  ")
          }
          // TODO
          // else if (f.dataType.isInstanceOf[ArrayType]) {
          //   println(s"${prefix}${f.name} (Array)")
          //   prettyPrintSchema(f.dataType.asInstanceOf[ArrayType].elementType, prefix + "  ")
          // }
          else {
            println(s"${prefix}${f.name} ${f.dataType}")
          }
      }  
    }
  }

  test("Schema OSDU AbstractContact") {
    val source = Source.fromURL(getClass.getResource("/AbstractContact.1.0.0.json")).getLines.mkString
    val data = new JSON().deserialize[Map[String, Object]](source, classOf[Map[String, Object]])

    val actual = OSDUSchemaConverter.toStruct(data)

    val expected = StructType(
        StructField("Name" ,StringType, true) :: 
        StructField("OrganisationID", StringType, true) :: 
        StructField("RoleTypeID", StringType, true) :: 
        StructField("PhoneNumber", StringType, true) :: 
        StructField("Comment", StringType, true) :: 
        StructField("EmailAddress", StringType, true) :: Nil)

    assert(stripSchema(actual) == expected, "not equal schemas")

    assert(
      actual("OrganisationID").metadata.getString("pattern") ==
      "^[\\w\\-\\.]+:master-data\\-\\-Organisation:[\\w\\-\\.\\:\\%]+:[0-9]*$",
      "pattern metadata doesn't match")
  }

  test("Schema OSDU AbstractCoordinates") {
    val source = Source.fromURL(getClass.getResource("/AbstractCoordinates.1.0.0.json")).getLines.mkString
    val data = new JSON().deserialize[Map[String, Object]](source, classOf[Map[String, Object]])

    val actual = OSDUSchemaConverter.toStruct(data)

    val expected = StructType(
        StructField("Y", DoubleType, true) :: 
        StructField("X", DoubleType, true) :: Nil)

    assert(stripSchema(actual) == expected, "not equal schemas")
  }

  test("Schema OSDU GeoSchema") {
    val source = Source.fromURL(getClass.getResource("/GetGeoSchema-Response.json")).getLines.mkString
    val data = new JSON().deserialize[Map[String, Object]](source, classOf[Map[String, Object]])

    val actual = OSDUSchemaConverter.toStruct(data)

    val expected = StructType(
      StructField("otherRelevantDataCountries", ArrayType(StringType, true), false) ::
      StructField("createUser", StringType, true) ::
      StructField("legaltags", ArrayType(StringType, true), false) ::
      StructField("createTime", StringType, true) ::
      StructField(
        "data", 
        StructType(
          StructField("GeoPoliticalEntityName", StringType, true) ::
          StructField("GeoPoliticalEntityID", StringType, true) ::
          StructField("DisputedIndicator", BooleanType, true) ::
          StructField("EffectiveDate", StringType, true) ::
          StructField("TerminationDate", StringType, true) ::
          StructField("ParentGeoPoliticalEntityID", StringType, true) ::
          StructField(
            "GeoPoliticalEntityNameAliases", 
            ArrayType(
              StructType(
                StructField("AliasName", StringType, true) ::
                StructField("DefinitionOrganisationID", StringType, true) ::
                StructField("AliasNameTypeID", StringType, true) ::
                StructField("TerminationDateTime", StringType, true) ::
                StructField("EffectiveDateTime", StringType, true) :: Nil
              ),
              true),
            true) ::
          StructField("GeoPoliticalEntityTypeID", StringType, true) ::
          StructField("DaylightSavingTimeStartDate", StringType, true) ::
          StructField("DaylightSavingTimeEndDate", StringType, true) :: Nil),
          true) ::
      StructField("parents", ArrayType(StringType, true), true) ::
      StructField("owners", ArrayType(StringType, true), false) ::
      StructField("modifyUser", StringType, true) ::
      StructField("viewers", ArrayType(StringType, true), false) ::
      StructField("version", IntegerType, true) ::
      StructField("id", StringType, true) ::
      StructField(
        "meta",
        ArrayType(
          StructType(
            StructField("name", StringType, true) ::
            StructField("unitOfMeasureID", StringType, true) ::
            StructField("coordinateReferenceSystemID", StringType, true) ::
            StructField("persistableReference", StringType, false) ::
            StructField("propertyNames", ArrayType(StringType, true), true) :: Nil),
          true),
        true) ::
      StructField("modifyTime", StringType, true) ::
      StructField("status", StringType, true) ::
      StructField("kind", StringType, false) :: Nil)

    assert(stripSchema(actual) == expected, "not equal schemas")
  }
}