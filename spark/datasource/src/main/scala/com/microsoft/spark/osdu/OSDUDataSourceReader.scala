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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataSourceReader, InputPartition, InputPartitionReader}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, StringType, BooleanType, IntegerType, StructField, StructType}
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import java.util.{ArrayList, Map, List, UUID}

import org.apache.spark.sql.sources.v2.reader.{SupportsPushDownFilters, SupportsPushDownRequiredColumns}

import osdu.client.{ApiClient, Configuration}
import osdu.client.api.SchemaApi

@SerialVersionUID(1L)
class OSDUDataSourceReader(options: DataSourceOptions)
  extends DataSourceReader with Serializable with SupportsPushDownRequiredColumns { // with SupportsPushDownFilters {
  private val logger = Logger.getLogger(classOf[OSDUDataSourceReader])

  private val defaultMaxPartitions = 1

  private val kind = options.get("kind").orElse("")
  private val query = options.get("query").orElse("")
  private val oakApiEndpoint = options.get("oakApiEndpoint").get
  private val partitionId = options.get("partitionId").get
  private val bearerToken = options.get("bearerToken").get

  var prunedSchema: Option[StructType] = None

   override def pruneColumns(requiredSchema: StructType): Unit = {
    //  println(s"Discovered Schema $schemaForKind")
    //  println(s"RequiredSchema    $requiredSchema")
     prunedSchema = Some(requiredSchema)
   }

  private lazy val schemaForKind = {
    val client = new ApiClient()

    client.setBasePath(oakApiEndpoint)
    client.setApiKey(bearerToken)
    client.setApiKeyPrefix("Bearer")
    client.addDefaultHeader("data-partition-id", partitionId)

    val schemaApi = new SchemaApi(client)

    val schema = schemaApi.getSchema(partitionId, kind).asInstanceOf[Map[String, Object]]

    // definitions are top-level
    var definitions = schema.get("definitions").asInstanceOf[Map[String, Map[String, Object]]]

    def osduSchemaToStruct(obj: Map[String, Object]): Option[StructType] = {
      // println(s"osduSchemaToStruct $obj") // use for debugging

      // fetch mandatory fields and translate to nullable flag
      val required = obj.getOrDefault("required", new ArrayList[String]()).asInstanceOf[List[String]]

      val props = obj.get("properties").asInstanceOf[Map[String, Map[String, Object]]].asScala
      if (props == null) {
        // from a global/union perpsective allOf/anyOf has the same behavior
        var axxOf = obj.get("allOf").asInstanceOf[List[Map[String, Object]]]
        if (axxOf == null) 
          axxOf = obj.get("anyOf").asInstanceOf[List[Map[String, Object]]]
        if (axxOf == null)
          axxOf = obj.get("oneOf").asInstanceOf[List[Map[String, Object]]]

        if (axxOf == null)
          None
        else {
          // union all properties of objects contained in allOf list
          val subFields = axxOf.asScala.flatMap { osduSchemaToStruct(_).map { _.fields } }
          // flatten - distinctBy name
          Some(new StructType(subFields.flatten.toList.groupBy(_.name).map(_._2.head).toArray))
        }
      }
      else {
        val fields = props.flatMap { 
          case (propertyName, obj) => {
            var refStr = obj.get("$ref").asInstanceOf[String];
            if (refStr != null) {
              // inject definition
              osduSchemaToStruct(definitions.get(refStr.substring("#/definitions/".length))).get.fields
            }
            else {
              def resolveDataType(subObj: Map[String, Object]): Option[DataType] = {
                subObj.get("type").asInstanceOf[String] match {
                  case "string"  => Some(StringType)
                  case "boolean" => Some(BooleanType)
                  case "integer" => Some(IntegerType)
                  case "object" => osduSchemaToStruct(subObj)
                  case "array" => { 
                    val itemData = subObj.get("items").asInstanceOf[Map[String, Object]]
                    // resolve primitive & complex types
                    val elemType = resolveDataType(itemData).getOrElse({
                      // resolve reference types
                      val refStr = itemData.get("$ref").asInstanceOf[String].substring("#/definitions/".length)
                      // recurse
                      osduSchemaToStruct(definitions.get(refStr)).get
                    })
                    Some(ArrayType(elemType, true))
                  }
                  case null => osduSchemaToStruct(subObj)
                  case _ => {
                    // val typeAny = obj.get("type")
                    // println(s"UNSUPPORTED TYPE $typeAny")
                    None
                  }
                }
              }

              val resolvedDataType = resolveDataType(obj)
              val comment = obj.get("description").asInstanceOf[String]
              val isNullable = !required.contains(propertyName)

              resolvedDataType.map { StructField(propertyName, _, isNullable).withComment(comment) }
            }
          }
        }
        // 2.13.x
        // Some(new StructType(fields.toSeq.distinctBy { _.name } .toArray))

        // 2.12.x
        Some(new StructType(fields.toList.groupBy(_.name).map(_._2.head).toArray))
      }

      // TODO: de-duplicate fields of the same name (can happen w/ inheritance/abstract/...)

    }

  // TODO: why is this executed twice???
    osduSchemaToStruct(schema).get
  }


  def readSchema: StructType = prunedSchema.getOrElse(schemaForKind)

//   override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//     // unfortunately predicates on nested elements are not pushed down by Spark
//     // https://issues.apache.org/jira/browse/SPARK-17636
//     // https://github.com/apache/spark/pull/22535

//     val jsonSchema = AvroUtil.catalystSchemaToJson(schemaWithOutRowKey)
//     val result = new FilterToJuel(jsonSchema.attributeToVariableMapping, rowKeyColumn)
//       .serializeFilters(filters, options.get("filter").orElse(""))

//     this.filters = result.supportedFilters.toArray

//     if (result.serializedFilter.length > 0) {
//       this.filterInJuel = Some("${" + result.serializedFilter + "}")
//       logger.info(s"JUEL filter: ${this.filterInJuel}")
//     }

//     result.unsupportedFilters.toArray
//   }

//   override def pushedFilters(): Array[Filter] = filters

  def planInputPartitions: List[InputPartition[InternalRow]] = {
    // TODO: we could return multiple readers to parallelize the invocation, but we'd loose consistency as I'm not sure how to share the cursor

    new ArrayList[InputPartition[InternalRow]](Seq(new PartitionReaderFactory(kind, query, oakApiEndpoint, partitionId, bearerToken,
      prunedSchema.getOrElse(schemaForKind))).asJava)
  }
}

class PartitionReaderFactory(
  kind: String,
  query: String,
  oakApiEndpoint: String,
  partitionId: String,
  bearerToken: String,
  schema: StructType
  )
  extends InputPartition[InternalRow] {

  def createPartitionReader: InputPartitionReader[InternalRow] = {

    Logger.getLogger(classOf[OSDUDataSourceReader]).info(s"Partition reader for $query")

    new OSDUInputPartitionReader(kind, query, oakApiEndpoint, partitionId, bearerToken, schema)
  }
}