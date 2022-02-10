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

import java.util.Optional

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.types.StructType

/** Spark DataSourceV2 implementation for OSDU Record API */
class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  override def createReader(schema: StructType, options: DataSourceOptions): DataSourceReader = {
    throw new UnsupportedOperationException("Not supported")
  }

  override def createReader(options: DataSourceOptions): DataSourceReader = 
    new OSDUDataSourceReader(options)

  override def createWriter(jobId: String,
                            schema: StructType,
                            mode: SaveMode,
                            options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new OSDUDataSourceWriter(schema, mode, options))
  }
}