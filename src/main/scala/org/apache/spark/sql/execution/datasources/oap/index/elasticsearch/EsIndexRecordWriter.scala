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

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

class EsIndexRecordWriter(
  configuration: Configuration,
  indexPath: Path,
  keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  private val client = OapEsClientRepository.getOrCreateOapEsClient()
  private var recordCount = 0

  private val bufferedRecords = new ArrayBuffer[(String, Int)]()

  private def flushBufferedRows(): Unit = {
    client.batchInsert(bufferedRecords, indexPath.getName, OapEsProperties.ES_INDEX_NAME)
    bufferedRecords.clear()
  }

  override def close(taskAttemptContext: TaskAttemptContext): Unit = {
    flushBufferedRows()
    // Just touch a file without any content
    val file = indexPath.getFileSystem(configuration).create(indexPath)
    file.close()
  }

  override def write(k: Void, v: InternalRow): Unit = {
    if (bufferedRecords.length == 100) {
      flushBufferedRows()
    }
    bufferedRecords.append((v.get(0, keySchema.head.dataType).toString, recordCount))
    recordCount += 1
  }
}
