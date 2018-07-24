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

package org.apache.spark.sql.execution.datasources.oap.statistics

import java.io.OutputStream

import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index.RangeInterval
import org.apache.spark.sql.types.StructType

private[oap] case class Sample(key: Key, nEq: Int, nLt: Int, nDlt: Int, isPeriodic: Boolean)

private[oap] class SampleStatisticsReader(schema: StructType) extends StatisticsReader(schema) {
  override val id: Int = StatisticsType.TYPE_SQLITE_SAMPLE

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    0
  }
  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    StatsAnalysisResult.USE_INDEX
  }
}

private[oap] class SampleStatisticsWriter(
    schema: StructType,
    conf: Configuration) extends StatisticsWriter(schema, conf) {
  override val id: Int = StatisticsType.TYPE_SQLITE_SAMPLE

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {
    0
  }
}