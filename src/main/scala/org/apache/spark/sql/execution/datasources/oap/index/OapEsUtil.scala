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

import org.elasticsearch.client.transport.TransportClient

import org.apache.spark.sql.execution.datasources.oap.Key

/**
 * This should be a wrapper of [[TransportClient]], providing several APIs to access ES server.
 */
trait OapEsUtil {

  /**
   * client to send requests to ES server
   */
  protected def transportClient: TransportClient

  /**
   * create an ES index for one OAP table
   * ES Table Index is not equal to OAP index
   * All OAP indexes built for one table is stored in one ES Table Index file
   * We only create ES Table Index once for each table
   *
   * @param tableName table name (corresponding to one ES table index)
   */
  def createEsTableIndex(tableName: String): Unit

  /**
   * Check if the ES table index has already been created
   *
   * @param tableName table name (corresponding to one ES table index)
   * @return true if the index is already created.
   */
  protected def checkEsTableIndexExists(tableName: String): Boolean

  /**
   * This is the function used to create OAP index. It may be called several time to build
   * one OAP index file. We need to store value, rowId, indexFileName for each data record.
   *
   * @param records From data file, it's an array of (value, rowId) pair
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param tableName table name (corresponding to one ES table index)
   * @return successfully inserted row count
   */
  def batchInsert(records: Seq[(Key, Int)], oapIndexFileName: String, tableName: String): Int

  /**
   * Drop all ES Index records belong to one OAP index file (dataFileName + indexName)
   *
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @param tableName table name (corresponding to one ES table index)
   * @return successfully deleted row count
   */
  def batchDelete(oapIndexFileName: String, tableName: String): Int

  /**
   * Drop all ES Index records belong to some OAP index files (dataFileName + indexName)
   * @param oapIndexFileNames Array of Index File names
   * @param tableName table name (corresponding to one ES table index)
   * @return successfully deleted row count
    */
  def batchDelete(oapIndexFileNames: Seq[String], tableName: String): Int

  /**
   * Use ES scroll fetch API to return results. It may need several times to fetch all results
   *
   * @param query query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @return row ids satisfy the query
   */
  def scrollFetch(query: String, oapIndexFileName: String): Seq[Int]

  /**
   * Fetch all results
   *
   * @param query query
   * @param oapIndexFileName Index File name is the composition of indexName and dataFileName
   * @return row ids satisfy the query
   */
  def fullFetch(query: String, oapIndexFileName: String): Seq[Int]
}
