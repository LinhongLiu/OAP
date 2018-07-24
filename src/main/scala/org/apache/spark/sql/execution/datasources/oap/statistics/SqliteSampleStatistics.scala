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

import java.io.{ByteArrayOutputStream, OutputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.oap.Key
import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache
import org.apache.spark.sql.execution.datasources.oap.index.{IndexUtils, RangeInterval}
import org.apache.spark.sql.types.StructType

/**
 * Sample Statistics in light of sqlite implementation
 * nEq: the number of rows equal to the sample row
 * nLt: the number of rows less than the sample row
 * nDlt: the number of distinct rows less than the sample row
 * Sample size is 1% of the total rows.
 * 1/3 of samples is selected from the rows evenly. called periodic sample
 * 2/3 of samples is the ones with greatest nEq. called "better" sample
 */
private[oap] case class Sample(key: Key, nEq: Int, nLt: Int, nDlt: Int, isPeriodic: Boolean)

private[oap] class SampleStatisticsReader(schema: StructType) extends StatisticsReader(schema) {
  override val id: Int = StatisticsType.TYPE_SQLITE_SAMPLE

  protected var sampleArray: Array[Sample] = _
  protected var rowCount: Int = 0
  protected var avgEq: Int = 0

  override def read(fiberCache: FiberCache, offset: Int): Int = {
    var readOffset = super.read(fiberCache, offset) + offset

    rowCount = fiberCache.getInt(readOffset)
    val size = fiberCache.getInt(readOffset + 4)

    sampleArray = new Array[Sample](size)
    readOffset += 8
    var rowOffset = 0
    for (i <- 0 until size) {
      val start = readOffset + size * IndexUtils.INT_SIZE + rowOffset
      val (key, length) = nnkr.readKey(fiberCache, start)
      val nEq = fiberCache.getInt(start + length)
      val nLt = fiberCache.getInt(start + length + IndexUtils.INT_SIZE)
      val nDLt = fiberCache.getInt(start + length + IndexUtils.INT_SIZE * 2)
      sampleArray(i) = Sample(key, nEq, nLt, nDLt, isPeriodic = false)

      rowOffset = fiberCache.getInt(readOffset + i * IndexUtils.INT_SIZE)
    }
    avgEq = sampleArray.map(_.nEq).sum / sampleArray.length

    readOffset += (rowOffset + size * IndexUtils.INT_SIZE)
    readOffset - offset
  }

  override def analyse(intervalArray: ArrayBuffer[RangeInterval]): StatsAnalysisResult = {
    StatsAnalysisResult.USE_INDEX
  }
}

private[oap] class SampleStatisticsWriter(
    schema: StructType,
    conf: Configuration) extends StatisticsWriter(schema, conf) {

  override val id: Int = StatisticsType.TYPE_SQLITE_SAMPLE

  private val MIN_SAMPLE = 24
  private val SAMPLE_RATIO = 0.01

  protected var sampleArray: Seq[Sample] = _

  override def write(writer: OutputStream, sortedKeys: ArrayBuffer[Key]): Int = {

    val rowCount = sortedKeys.length
    // Sample size is 1% of rowCount. at least 24 sample in case too small row count.
    val mxSample = math.max(MIN_SAMPLE, (rowCount * SAMPLE_RATIO + 1).toInt)


    sampleArray = takeSample(sortedKeys, mxSample)


    var offset = super.write(writer, sortedKeys)
    IndexUtils.writeInt(writer, rowCount)
    IndexUtils.writeInt(writer, sampleArray.length)
    val tempWriter = new ByteArrayOutputStream()
    sampleArray.foreach { s =>
      nnkw.writeKey(tempWriter, s.key)
      IndexUtils.writeInt(tempWriter, s.nEq)
      IndexUtils.writeInt(tempWriter, s.nLt)
      IndexUtils.writeInt(tempWriter, s.nDlt)
      IndexUtils.writeInt(writer, tempWriter.size())
    }
    offset += sampleArray.length * IndexUtils.INT_SIZE
    writer.write(tempWriter.toByteArray)
    offset += tempWriter.size()
    offset
  }

  /**
   * Calculate the nEq, nLt, nDlt for each key. Pick out the best `mxSample` samples
   * For normal samples, select the ones with greater `nEq`
   * For periodic samples, always select.
   * There are 2 times sort in this function. but the array length is far less than the row count.
   */
  private def takeSample(keys: ArrayBuffer[Key], mxSample: Int): Seq[Sample] = {
    // Periodic sample divisor
    val nPSample = keys.length / (mxSample / 3 + 1) + 1

    // Handle the first row
    implicit val ord: Ordering[Sample] = Ordering[(Boolean, Int)].on(s => (!s.isPeriodic, -s.nEq))
    val samples = new mutable.PriorityQueue[Sample]()

    // Handle the first row
    var nEq = 1
    var nLt = 0
    var nDLt = 0
    var isPeriodic = false
    // Go through from the second row. Insert a sample until see a different key.
    (1 until keys.length).foreach { i =>
      if (i % nPSample == 0) isPeriodic = true
      if (keys(i - 1) == keys(i)) {
        nEq += 1
      } else {
        samples.enqueue(Sample(keys(i - 1), nEq, nLt, nDLt, isPeriodic = isPeriodic))
        if (samples.length > mxSample) samples.dequeue()
        nLt += nEq
        nEq = 1
        nDLt += 1
        isPeriodic = false
      }
    }
    // Insert the last sample
    samples.enqueue(Sample(keys.last, nEq, nLt, nDLt, isPeriodic = isPeriodic))
    if (samples.length > mxSample) samples.dequeue()

    // Ordering the samples by original order.
    val ordering = GenerateOrdering.create(schema)
    samples.dequeueAll.sortWith((l, r) => ordering.compare(l.key, r.key) < 0)
  }
}
