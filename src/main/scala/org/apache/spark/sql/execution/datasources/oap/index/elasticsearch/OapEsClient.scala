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

import org.elasticsearch.{ResourceAlreadyExistsException, ResourceNotFoundException}
import org.elasticsearch.client.transport.TransportClient

trait OapEsClient {

  /**
   * client to send requests to ES server
   */
  protected def transportClient: TransportClient

  /**
   * create an ES index.
   *
   * In OAP, each data file is related to N index files, which N is the index number.
   * But in ES, it's different. We only use 1 index in ES to store all records from OAP.
   * Each record will have 3 fields in ES: row_id, value, oap_index_file_name.
   * `oap_index_file_name` is exactly same to OAP's BTREE and BITMAP index file name.
   * We use this to indicate the record is from which data file and which column
   *
   * @param indexName index which stores OAP records
   * @throws ResourceAlreadyExistsException if indexName is already exist
   */
  protected[elasticsearch] def createEsIndex(indexName: String): Unit

  /**
   * Drop ES index. This will delete all data inserted by OAP.
   *
   * @param indexName index which stores OAP records*
   * @throws ResourceNotFoundException if indexName is not exist
   */
  protected[elasticsearch] def dropEsIndex(indexName: String): Unit

  /**
   * Check if the ES table index has already been created
   *
   * @param indexName index which stores OAP records
   * @return true if the index is already created.
   */
  protected[elasticsearch] def checkEsIndexExists(indexName: String): Boolean

  /**
   * Insert records into ES. Inserted records will be indexed by ES.
   * When OAP creates index, it will generate an index file name using a combination with
   * data file name and index name. Use this info, we will know the records from ES is from
   * which data file and which column.
   * For all record values, should be string type.
   *
   * @param records From data file, it's an array of (value, rowId) pair
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param indexName index which stores OAP records
   * @return successfully inserted row count
   */
  def batchInsert(records: Seq[(String, Int)], oapIndexFileName: String, indexName: String): Int

  /**
   * Drop all ES Index records belong to one OAP index file (dataFileName + indexName)
   *
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param indexName index which stores OAP records
   * @return successfully deleted row count
   */
  def batchDelete(oapIndexFileName: String, indexName: String): Int

  /**
   * Use ES scroll fetch API to return results. It may need several times to fetch all results
   *
   * @param query query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param indexName index which stores OAP records
   * @return row ids satisfy the query
   */
  def scrollFetch(query: String, oapIndexFileName: String, indexName: String): Seq[Int]

  /**
   * Fetch all results
   *
   * @param query query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param indexName index which stores OAP records
   * @return row ids satisfy the query
   */
  def fullFetch(query: String, oapIndexFileName: String, indexName: String): Seq[Int]
}

