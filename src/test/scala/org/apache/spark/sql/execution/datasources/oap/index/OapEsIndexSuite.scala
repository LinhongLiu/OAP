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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.unsafe.types.UTF8String

class OapEsIndexSuite extends SharedOapContext {

  private val TEST_HOST = "127.0.0.1"
  private val TEST_PORT = 9300
  private val TEST_TABLE_NAME = "test_table"
  private val es = new OapEsIndex(TEST_HOST, TEST_PORT)

  test("index exists check") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    assert(es.checkEsTableIndexExists(TEST_TABLE_NAME))
    es.dropESTableIndex(TEST_TABLE_NAME)
    assert(!es.checkEsTableIndexExists(TEST_TABLE_NAME))
  }

  test("es table index create") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    assert(es.checkEsTableIndexExists(TEST_TABLE_NAME))
    es.dropESTableIndex(TEST_TABLE_NAME)
  }

  test("es table index drop") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    es.dropESTableIndex(TEST_TABLE_NAME)
    assert(!es.checkEsTableIndexExists(TEST_TABLE_NAME))
  }

  test("insert records to es table") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    val records = (1 to 10).map { x =>
      val key = InternalRow.fromSeq(UTF8String.fromString(s"test$x") :: Nil)
      (key, x)
    }
    es.batchInsert(records, "data.idx1.index", TEST_TABLE_NAME)
    val ids = es.queryAllRecordIds(TEST_TABLE_NAME)
    assert(ids.toSet == (1 to 10).map(_.toString).toSet)
    es.dropESTableIndex(TEST_TABLE_NAME)
  }

  test("query records ids by index file name") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    val records1 = (1 to 10).map { x =>
      val key = InternalRow.fromSeq(UTF8String.fromString(s"test$x") :: Nil)
      (key, x)
    }
    es.batchInsert(records1, "data.idx1.index", TEST_TABLE_NAME)

    val records2 = (11 to 20).map { x =>
      val key = InternalRow.fromSeq(UTF8String.fromString(s"test$x") :: Nil)
      (key, x)
    }
    es.batchInsert(records2, "data.idx2.index", TEST_TABLE_NAME)

    val ids = es.queryIdsByIndexFileName("data.idx1.index", TEST_TABLE_NAME)
    assert(ids.toSet == (1 to 10).map(_.toString).toSet)
    es.dropESTableIndex(TEST_TABLE_NAME)
  }

  test("drop records for oap index from es") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    val records1 = (1 to 10).map { x =>
      val key = InternalRow.fromSeq(UTF8String.fromString(s"test$x") :: Nil)
      (key, x)
    }
    es.batchInsert(records1, "data.idx1.index", TEST_TABLE_NAME)

    val records2 = (11 to 20).map { x =>
      val key = InternalRow.fromSeq(UTF8String.fromString(s"test$x") :: Nil)
      (key, x)
    }
    es.batchInsert(records2, "data.idx2.index", TEST_TABLE_NAME)

    es.batchDelete("data.idx1.index", TEST_TABLE_NAME)

    val ids = es.queryAllRecordIds(TEST_TABLE_NAME)
    assert(ids.toSet == (11 to 20).map(_.toString).toSet)
    es.dropESTableIndex(TEST_TABLE_NAME)
  }
}
