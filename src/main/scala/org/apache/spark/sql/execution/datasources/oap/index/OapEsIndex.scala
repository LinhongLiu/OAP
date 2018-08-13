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

package org.apache.spark.sql.execution.datasources.oap.index

import java.net.InetAddress

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.index.query.QueryBuilders

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.parquet.OapTransportClient
import org.apache.spark.unsafe.types.UTF8String

class OapEsIndex(host: String, port: Int) extends OapEsUtil with Logging {

  // We only use one document type in ES.
  private val docType = "oap_records"

  private val client = getEsClient

  private val mappings = {
    XContentFactory.jsonBuilder()
      .startObject()
        // Settings of this Index
        .startObject("settings")
          .field("number_of_shards", 1)
          .field("number_of_replicas", 0)
        .endObject()
        .startObject("mappings")
          // This is the document type, called "oap_records"
          .startObject("oap_records")
            .startObject("properties")
              // value in indexed column.
              .startObject("key")
                .field("type", "text")
                .field("store", "yes")
                .field("index", "analyzed")
              .endObject()
              // index file name = data file name + index name
              .startObject("index_file_name")
                .field("type", "string")
                .field("store", "yes")
                .field("index", "not_analyzed")
              .endObject()
              // The row id of this record in data file
              .startObject("rowId")
                .field("type", "integer")
                .field("store", "yes")
              .endObject()
            .endObject()
          .endObject()
        .endObject()
      .endObject()
  }

  /**
   * Drop the whole table index. Usually, we don't need this.
   */
  def dropESTableIndex(tableIndexName: String): Unit = {

    if (checkEsTableIndexExists(tableIndexName)) {

      client.admin().indices()
        .delete(new DeleteIndexRequest(tableIndexName))
        .actionGet()
    } else {

      logWarning(s"ES Index: $tableIndexName is not exist")
    }
  }

  /**
   * This is the function used to create OAP index. It may be called several time to build
   * one OAP index file. We need to store value, rowId, indexFileName for each data record.
   *
   * Since we use ES Index to store all indexes for one table, we need an identifier to locate
   * one specific OAP index file. So, data file name and column name is needed.
   *
   * @param records From data file, it's an array of (value, rowId) pair
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param tableName table name (corresponding to one ES table index)
   * @return successfully inserted row count
   */
  override def batchInsert(
    records: Seq[(Key, Int)], oapIndexFileName: String, tableName: String): Int = {

    val bulkRequest = client.prepareBulk()

    records.foreach { case (key, rowId) =>
      val obj = XContentFactory.jsonBuilder()
        .startObject()
        .field("key", key.getUTF8String(0))
        .field("index_file_name", oapIndexFileName)
        .field("rowId", rowId)
        .endObject()
      val indexRequest = client.prepareIndex(tableName, docType)
        .setSource(obj).setId(rowId.toString)
      bulkRequest.add(indexRequest)
    }
    bulkRequest.execute().actionGet()
    client.admin().indices().refresh(new RefreshRequest(tableName)).actionGet()
    records.length
  }

  def queryAllRecordIds(tableIndexName: String): Seq[String] = {
    val queryBuilder = QueryBuilders.matchAllQuery()
    val response = client.prepareSearch(tableIndexName)
      .setTypes(docType)
      .addDocValueField("rowId")
      .setQuery(queryBuilder).setSize(100).execute().actionGet()

    response.getHits.getHits.map(_.getId)
  }

  def queryIdsByIndexFileName(oapIndexFileName: String, tableIndexName: String): Seq[String] = {

    val queryBuilder = QueryBuilders.termQuery("index_file_name", oapIndexFileName)
    val response = client.prepareSearch(tableIndexName)
      .setTypes(docType)
      .addDocValueField("rowId")
      .setQuery(queryBuilder).setSize(100).execute().actionGet()

    response.getHits.getHits.map(_.getId)
  }

  /**
   * Pick one idle connection from connection pool
   * TODO: Should be a pool
   *
   * @return the idle ES TransportClient
   */
  override protected def getEsClient: TransportClient = {
    val settings = Settings.builder
      .put("cluster.name", "elasticsearch")
      .put("client.transport.sniff", true)
      .put("client.transport.ignore_cluster_name", false)
      .put("client.transport.ping_timeout", "5s")
      .put("client.transport.nodes_sampler_interval", "5s").build

    val client = new OapTransportClient(settings)
    val address = new InetSocketTransportAddress(InetAddress.getByName(host), port)

    client.addTransportAddress(address)
    client.connectedNodes()

    client
  }

  /**
   * create an ES index for one OAP table
   * ES Table Index is not equal to OAP index
   * All OAP indexes built for one table is stored in one ES Table Index file
   * We only create ES Table Index once for each table
   *
   * @param tableName table name (corresponding to one ES table index)
   */
  override def createEsTableIndex(tableName: String): Unit = {

    if (!checkEsTableIndexExists(tableName)) {

      client.admin().indices()
        .prepareCreate(tableName)
        .setSource(mappings)
        .execute().actionGet()
    } else {
      logWarning(s"ES Index: $tableName is already exist")
    }
  }

  /**
   * Check if the ES table index has already been created
   *
   * @param tableName table name (corresponding to one ES table index)
   * @return true if the index is already created.
   */
  override def checkEsTableIndexExists(tableName: String): Boolean = {
    client.admin().indices()
      .exists(new IndicesExistsRequest(tableName))
      .actionGet().isExists
  }

  /**
   * Drop all ES Index records belong to one OAP index file (dataFileName + indexName)
   *
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param tableName        table name (corresponding to one ES table index)
   * @return successfully deleted row count
   */
  override def batchDelete(oapIndexFileName: String, tableName: String): Int = {
    val ids = queryIdsByIndexFileName(oapIndexFileName, tableName)

    val bulkRequest = client.prepareBulk()

    ids.foreach { id =>
      bulkRequest.add(client.prepareDelete(tableName, docType, id.toString))
    }
    bulkRequest.execute().actionGet()
    client.admin().indices().refresh(new RefreshRequest(tableName)).actionGet()
    ids.length
  }

  /**
   * Drop all ES Index records belong to some OAP index files (dataFileName + indexName)
   *
   * @param oapIndexFileNames Array of Index File names
   * @param tableName         table name (corresponding to one ES table index)
   * @return successfully deleted row count
   */
  override def batchDelete(oapIndexFileNames: Seq[String], tableName: String): Int = {
    oapIndexFileNames.map(batchDelete(_, tableName)).sum
  }

  /**
   * Use ES scroll fetch API to return results. It may need several times to fetch all results
   *
   * @param query            query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @return row ids satisfy the query
   */
  override def scrollFetch(query: String, oapIndexFileName: String): Seq[Int] =
    throw new NotImplementedError()

  /**
   * Fetch all results
   *
   * @param query            query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @return row ids satisfy the query
   */
  override def fullFetch(query: String, oapIndexFileName: String): Seq[Int] =
    throw new NotImplementedError()
}
