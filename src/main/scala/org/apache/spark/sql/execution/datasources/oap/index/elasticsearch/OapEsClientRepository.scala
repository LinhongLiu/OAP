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

package org.apache.spark.sql.execution.datasources.oap.index.elasticsearch

import java.net.InetAddress

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

import org.apache.spark.SparkEnv
import org.apache.spark.sql.execution.datasources.parquet.OapTransportClient
import org.apache.spark.sql.internal.oap.OapConf

object OapEsClientRepository {

  private val esHost = SparkEnv.get.conf.get(OapConf.OAP_ELASTICSEARCH_HOST)
  private val esPort = SparkEnv.get.conf.get(OapConf.OAP_ELASTICSEARCH_PORT)

  private val esClientSettings = {
    Settings.builder
      .put("cluster.name", "elasticsearch")
      .put("client.transport.sniff", true)
      .put("client.transport.ignore_cluster_name", false)
      .put("client.transport.ping_timeout", "5s")
      .put("client.transport.nodes_sampler_interval", "5s").build
  }

//  initialization()
//
//  private def initialization() {
//    // Before starting, let's check if we have created an index for oap
//    val client = getOrCreateOapEsClient()
//    if (client.checkEsIndexExists(OapEsProperties.ES_INDEX_NAME)) {
//      client.createEsIndex(OapEsProperties.ES_INDEX_NAME)
//    }
//  }

  // Currently just return a new client instance.
  // TODO: we need a client pool for recycling.
  def getOrCreateOapEsClient(): OapEsClient = {

    val transportClient = new OapTransportClient(esClientSettings)
    val address = new InetSocketTransportAddress(InetAddress.getByName(esHost), esPort)

    transportClient.addTransportAddress(address)
    transportClient.connectedNodes()

    val client = OapEsClientImpl(transportClient)
    if (!client.checkEsIndexExists(OapEsProperties.ES_INDEX_NAME)) {
      client.createEsIndex(OapEsProperties.ES_INDEX_NAME)
    }
    client
  }
}
