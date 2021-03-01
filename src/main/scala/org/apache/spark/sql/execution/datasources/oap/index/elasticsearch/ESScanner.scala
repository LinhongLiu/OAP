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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.execution.datasources.oap.IndexMeta
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils}

case class ESScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  // TODO: return all rows in one data file
  override def totalRows(): Long = _totalRows

  @transient private var internalIter: Iterator[Int] = _
  @transient private var _totalRows: Long = _

  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {

    val client = OapEsClientRepository.getOrCreateOapEsClient()

    val indexPath = IndexUtils.getIndexFilePath(conf, dataPath, meta.name, meta.time)

    val rowIds = client.fullFetch("", indexPath.getName, OapEsProperties.ES_INDEX_NAME)
    _totalRows = rowIds.length

    internalIter = rowIds.toIterator

    this
  }

  override def hasNext: Boolean = internalIter.hasNext

  override def next(): Int = internalIter.next()
}
