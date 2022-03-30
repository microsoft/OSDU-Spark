/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.spark.osdu

class OSDUOptions(options: java.util.Map[String, String])   {

  lazy val kind = getRequiredArgument(options, "kind")
  val osduApiEndpoint = getRequiredArgument(options, "osduApiEndpoint")
  val partitionId = getRequiredArgument(options, "partitionId")

  def getBearerToken(): String = {
    if (options.containsKey("bearerToken"))
      options.get("bearerToken")
    else
      AuthUtil.getBearerToken(options)
  }

  private def getRequiredArgument(options: java.util.Map[String, String], arg: String): String = {
    val result = if (options.containsKey(arg)) options.get(arg) else  {
      throw new Exception(arg + "argument required")
    }
    result
  }
}
