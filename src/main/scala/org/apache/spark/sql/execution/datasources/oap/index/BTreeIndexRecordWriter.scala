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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.RecordWriter
import org.apache.parquet.format.CompressionCodec

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.OapIndexProperties.IndexVersion.IndexVersion
import org.apache.spark.sql.execution.datasources.oap.index.impl.IndexFileWriterImpl
import org.apache.spark.sql.types._

private[index] object BTreeIndexRecordWriter {
  def apply(
      configuration: Configuration,
      indexFile: Path,
      schema: StructType,
      codec: CompressionCodec,
      indexVersion: IndexVersion): BTreeIndexRecordWriter = {
    val writer = IndexFileWriterImpl(configuration, indexFile)
    indexVersion match {
      case IndexVersion.OAP_INDEX_V1 =>
        BTreeIndexRecordWriterV1(configuration, writer, schema)
      case IndexVersion.OAP_INDEX_V2 =>
        BTreeIndexRecordWriterV2(configuration, writer, schema, codec)
    }
  }
}

trait BTreeIndexRecordWriter extends RecordWriter[Void, InternalRow]
