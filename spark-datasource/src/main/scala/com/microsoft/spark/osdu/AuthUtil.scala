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

import com.fasterxml.jackson.databind.ObjectMapper

import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util

object AuthUtil {
  def getBearerToken(map: util.Map[String, String]): String = {
    if (map.containsKey("bearerToken"))
      map.get("bearerToken")
    else {
      val clientSecret = map.get("clientSecret")
      val clientId = map.get("clientId")
      val endpoint = map.get("oauthEndpoint")

      // send oauth request
      val clientSecretEncoded = URLEncoder.encode(clientSecret, "UTF-8")
      val postBody = s"grant_type=client_credentials&client_id=$clientId&client_secret=$clientSecretEncoded&scope=$clientId/.default"

      val conn = new URL(endpoint).openConnection.asInstanceOf[HttpURLConnection]
      conn.setRequestMethod("POST")
      conn.addRequestProperty("Content-Type", "application/x-www-form-urlencoded")
      conn.setDoOutput(true)
      conn.getOutputStream().write(postBody.getBytes)
      conn.connect

      val responseJson = scala.io.Source.fromInputStream(conn.getInputStream).mkString
      val response = new ObjectMapper().readValue[java.util.Map[String, String]](responseJson, classOf[java.util.Map[String, String]])
      response.get("access_token")
    }
  }

}
