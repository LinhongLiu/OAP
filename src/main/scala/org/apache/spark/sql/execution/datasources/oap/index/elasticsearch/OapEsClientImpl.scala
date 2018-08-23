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

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.index.query.QueryBuilders

case class OapEsClientImpl(transportClient: TransportClient) extends OapEsClient {

  // TODO: Change mappings to Map
  override protected[elasticsearch]
  def createEsIndex(indexName: String): Unit = {

    transportClient.admin().indices()
      .prepareCreate(indexName)
      .setSource(OapEsProperties.mappings)
      .execute().actionGet()
  }

  override protected[elasticsearch]
  def dropEsIndex(indexName: String): Unit = {
    transportClient.admin().indices()
      .delete(new DeleteIndexRequest(indexName))
      .actionGet()
  }

  override protected[elasticsearch]
  def checkEsIndexExists(indexName: String): Boolean = {
    transportClient.admin().indices()
      .exists(new IndicesExistsRequest(indexName))
      .actionGet().isExists
  }

  override def batchInsert(
    records: Seq[(String, Int)], oapIndexFileName: String, indexName: String): Int = {

    val bulkRequest = transportClient.prepareBulk()

    records.foreach { case (value, rowId) =>
      val obj = XContentFactory.jsonBuilder()
        .startObject()
        .field(OapEsProperties.VALUE_FIELD, value)
        .field(OapEsProperties.INDEX_FILE_NAME_FIELD, oapIndexFileName)
        .field(OapEsProperties.ROW_ID_FIELD, rowId)
        .endObject()
      val indexRequest = transportClient.prepareIndex(indexName, OapEsProperties.DOC_TYPE)
        .setSource(obj)
      bulkRequest.add(indexRequest)
    }
    bulkRequest.execute().actionGet()
    transportClient.admin().indices().refresh(new RefreshRequest(indexName)).actionGet()
    records.length
  }

  private def queryIdsByIndexFileName(oapIndexFileName: String, indexName: String): Seq[String] = {
    val queryBuilder = QueryBuilders.termQuery(
      OapEsProperties.INDEX_FILE_NAME_FIELD, oapIndexFileName)

    val response = transportClient.prepareSearch(indexName)
      .setTypes(OapEsProperties.DOC_TYPE)
      .setQuery(queryBuilder).setSize(100).execute().actionGet()

    response.getHits.getHits.map(_.getId)
  }

  override def batchDelete(oapIndexFileName: String, indexName: String): Int = {
    val ids = queryIdsByIndexFileName(oapIndexFileName, indexName)

    val bulkRequest = transportClient.prepareBulk()

    ids.foreach { id =>
      bulkRequest.add(transportClient.prepareDelete(indexName, OapEsProperties.DOC_TYPE, id))
    }
    bulkRequest.execute().actionGet()
    transportClient.admin().indices().refresh(new RefreshRequest(indexName)).actionGet()
    ids.length
  }

  override def scrollFetch(
    query: String, oapIndexFileName: String, indexName: String): Seq[Int] = Seq.empty

  override def fullFetch(
    query: String, oapIndexFileName: String, indexName: String): Seq[Int] = Seq.empty

}
