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
import com.fasterxml.jackson.databind.ObjectMapper

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
    val data = new ObjectMapper().readValue[Map[String, Object]](source, classOf[Map[String, Object]])

    val actual = OSDUSchemaConverter.toDataType(data).asInstanceOf[StructType]

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
   implicit val formats = org.json4s.DefaultFormats

   val source = Source.fromURL(getClass.getResource("/AbstractCoordinates.1.0.0.json")).getLines.mkString
   val data = new ObjectMapper().readValue[Map[String, Object]](source, classOf[Map[String, Object]])

   val actual = OSDUSchemaConverter.toDataType(data).asInstanceOf[StructType]

   val expected = StructType(
       StructField("X", DoubleType, true) ::
       StructField("Y", DoubleType, true) :: Nil)

   assert(stripSchema(actual) == expected, "not equal schemas")
 }

def isEqual(struct1: StructType, struct2: StructType): Boolean = {
  // println(s"struct1: $struct1")
  // println(s"struct2: $struct2")
  struct1.headOption match {
    case Some(field) => {
      if(field.dataType.typeName != "struct") {
        struct2.find(_ == field) match {
         case Some(matchedField) => isEqual(StructType(struct1.filterNot(_.name == field.name)), StructType(struct2.filterNot(_.name == field.name)))
         case None => {
           println(s"Unable to find $field")
          false
         }
        }
      } else {
        val isEqualContents = struct2.find(x => x.name == field.name && x.nullable == field.nullable && x.dataType.typeName == "struct") match {
          case Some(matchedField) => isEqual(field.dataType.asInstanceOf[StructType], matchedField.dataType.asInstanceOf[StructType])
          case None => { 
            println(s"isEqualContents $field")
            false
          }
        }
        if(isEqualContents) isEqual(StructType(struct1.filterNot(_.name == field.name)), StructType(struct2.filterNot(_.name == field.name))) else {
            println(s"isEqualContents 2 $field")
          false
        }
      }
    }
    case None => { 
      val r = struct2.size == 0
      if(!r) println("Match none is false")
      r
    }
  }
}

 test("Schema OSDU GeoSchema") {
   implicit val formats = org.json4s.DefaultFormats

   val source = Source.fromURL(getClass.getResource("/GetGeoSchema-Response.json")).getLines.mkString
   val data = new ObjectMapper().readValue[Map[String, Object]](source, classOf[Map[String, Object]])

   val actual = OSDUSchemaConverter.toDataType(data).asInstanceOf[StructType]

   val expected = StructType(
     StructField("otherRelevantDataCountries", ArrayType(StringType, true), false) ::
     StructField("createUser", StringType, true) ::
     StructField("legaltags", ArrayType(StringType, true), false) ::
     StructField("createTime", StringType, true) ::
     StructField("version", IntegerType, true) ::
     StructField("modifyTime", StringType, true) ::
     StructField("status", StringType, true) ::
     StructField("kind", StringType, false) ::
     StructField("tags", MapType(StringType, StringType, true)) ::
     StructField(
       "data",
       StructType(
         StructField("GeoPoliticalEntityName", StringType, true) ::
         StructField("GeoPoliticalEntityID", StringType, true) ::
         StructField("DisputedIndicator", BooleanType, true) ::
         StructField("EffectiveDate", StringType, true) ::
//         StructField("ExtensionProperties", MapType(StringType,StringType,true),true) ::
         StructField(
           "GeoPoliticalEntityNameAliases",
           ArrayType(
             StructType(
               StructField("AliasName", StringType, true) ::
               StructField("DefinitionOrganisationID", StringType, true) ::
               StructField("AliasNameTypeID", StringType, true) ::
               StructField("TerminationDateTime", StringType, true) ::
//               StructField("ExtensionProperties", MapType(StringType,StringType,true),true) ::
               StructField("EffectiveDateTime", StringType, true) :: Nil
             ),
             true),
           true) ::
         StructField("GeoPoliticalEntityTypeID", StringType, true) ::
         StructField("TerminationDate", StringType, true) ::
         StructField("ParentGeoPoliticalEntityID", StringType, true) ::
         StructField("DaylightSavingTimeStartDate", StringType, true) ::
         StructField("DaylightSavingTimeEndDate", StringType, true) :: Nil),
         true) ::
     StructField("parents", ArrayType(StringType, true), true) ::
     StructField("owners", ArrayType(StringType, true), false) ::
     StructField("modifyUser", StringType, true) ::
     StructField("viewers", ArrayType(StringType, true), false) ::
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
      Nil)


   println(expected.treeString(Int.MaxValue))
   println(actual.treeString(Int.MaxValue))
//    // println(expected)
//    //println(stripSchema(actual))
//
//    assert(isEqual(expected, stripSchema(actual)), "not equal schemas")
 }
}