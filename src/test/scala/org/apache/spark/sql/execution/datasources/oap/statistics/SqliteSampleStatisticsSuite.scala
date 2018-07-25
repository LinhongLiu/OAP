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

import java.io.ByteArrayOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.oap.index.{IndexScanner, IndexUtils}
import org.apache.spark.sql.types.StructType

class SqliteSampleStatisticsSuite extends StatisticsTest {

  class TestSqliteSampleWriter(schema: StructType)
    extends SampleStatisticsWriter(schema, new Configuration()) {

    def getSampleArray: Seq[Sample] = sampleArray
  }

  class TestSqliteSampleReader(schema: StructType) extends SampleStatisticsReader(schema) {

    def getSampleArray: Seq[Sample] = sampleArray
  }

  test("test write function") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // keys needs to be sorted

    val testSample = new TestSqliteSampleWriter(schema)
    testSample.write(out, keys.to[ArrayBuffer])

    var offset = 0
    val fiber = wrapToFiberCache(out)
    assert(fiber.getInt(offset) == StatisticsType.TYPE_SQLITE_SAMPLE)
    offset += 4
    val rowCount = fiber.getInt(offset)
    offset += 4
    val size = fiber.getInt(offset)
    offset += 4
    assert(rowCount == keys.length)
    assert(size == math.max(24, (rowCount * 0.01 + 1).toInt))

    var rowOffset = 0
    for (i <- 0 until size) {
      val start = offset + size * 4 + rowOffset
      val (key, length) = nnkr.readKey(fiber, start)
      val nEq = fiber.getInt(start + length)
      val nLt = fiber.getInt(start + length + 4)
      val nDLt = fiber.getInt(start + length + 4 * 2)
      rowOffset = fiber.getInt(offset + i * 4)
      assert(keys.contains(key))
      assert(nEq == 1)
      assert(key.getInt(0) == nLt + 1)
      assert(nLt == nDLt)
    }
  }

  test("read function test") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray // random order
    val size = (Random.nextInt() % 200 + 200) % 200 + 10 // assert nonEmpty sample
    assert(size >= 0 && size <= 300)

    IndexUtils.writeInt(out, StatisticsType.TYPE_SQLITE_SAMPLE)
    IndexUtils.writeInt(out, keys.length)
    IndexUtils.writeInt(out, size)

    val tempWriter = new ByteArrayOutputStream()
    for (idx <- 0 until size) {
      nnkw.writeKey(tempWriter, keys(idx))
      IndexUtils.writeInt(tempWriter, 1)
      IndexUtils.writeInt(tempWriter, keys(idx).getInt(0) - 1)
      IndexUtils.writeInt(tempWriter, keys(idx).getInt(0) - 1)
      IndexUtils.writeInt(out, tempWriter.size)
    }
    out.write(tempWriter.toByteArray)

    val fiber = wrapToFiberCache(out)

    val testSample = new TestSqliteSampleReader(schema)
    testSample.read(fiber, 0)

    val array = testSample.getSampleArray

    for (i <- array.indices) {
      assert(ordering.compare(keys(i), array(i).key) == 0)
      assert(array(i).nEq == 1)
      assert(array(i).nLt == keys(i).getInt(0) - 1)
      assert(array(i).nLt == array(i).nDLt)
    }
  }

  test("read and write") {
    val keys = (1 to 300).map(i => rowGen(i)).toArray

    val sampleWrite = new TestSqliteSampleWriter(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val sampleRead = new TestSqliteSampleReader(schema)
    sampleRead.read(fiber, 0)

    val array = sampleRead.getSampleArray

    for (i <- array.indices) {
      assert(keys.contains(array(i).key))
      assert(array(i).nEq == 1)
      assert(array(i).key.getInt(0) == array(i).nLt + 1)
      assert(array(i).nLt == array(i).nDLt)
    }
  }

  test("test analyze function") {
    val keys = (0 until 3000).map(_ => rowGen(Random.nextInt(300))).sortBy(_.getInt(0))
    val dummyStart = new JoinedRow(InternalRow(1), IndexScanner.DUMMY_KEY_START)
    val dummyEnd = new JoinedRow(InternalRow(300), IndexScanner.DUMMY_KEY_END)

    val sampleWrite = new TestSqliteSampleWriter(schema)
    sampleWrite.write(out, keys.to[ArrayBuffer])

    val fiber = wrapToFiberCache(out)

    val sampleRead = new TestSqliteSampleReader(schema)
    sampleRead.read(fiber, 0)

    generateInterval(rowGen(-10), rowGen(-1), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StatsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), rowGen(400), startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StatsAnalysisResult.USE_INDEX)

    generateInterval(dummyStart, dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray).coverage >= 0.9)

    generateInterval(dummyStart, rowGen(0),
      startInclude = true, endInclude = false)
    assert(sampleRead.analyse(intervalArray) == StatsAnalysisResult.USE_INDEX)

    generateInterval(dummyStart, rowGen(300),
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray).coverage >= 0.9)

    generateInterval(rowGen(0), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray).coverage >= 0.9)

    generateInterval(rowGen(1), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray).coverage >= 0.9)

    generateInterval(rowGen(300), dummyEnd,
      startInclude = false, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StatsAnalysisResult.USE_INDEX)

    generateInterval(rowGen(301), dummyEnd,
      startInclude = true, endInclude = true)
    assert(sampleRead.analyse(intervalArray) == StatsAnalysisResult.USE_INDEX)
  }
}
