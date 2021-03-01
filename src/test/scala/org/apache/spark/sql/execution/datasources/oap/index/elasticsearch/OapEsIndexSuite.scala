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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

class OapEsIndexSuite extends SharedOapContext {


  // Skip file leak checking
  override def beforeEach(): Unit = {}
  override def afterEach(): Unit = {}

  // scalastyle:off println
  test("test create index ddl") {
    val es = OapEsClientRepository.getOrCreateOapEsClient()
    val path = Utils.createTempDir().getAbsolutePath

    if (es.checkEsIndexExists(OapEsProperties.ES_INDEX_NAME)) {
      es.dropEsIndex(OapEsProperties.ES_INDEX_NAME)
    }
    es.createEsIndex(OapEsProperties.ES_INDEX_NAME)

    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
           | USING oap
           | OPTIONS (path '$path')""".stripMargin)

    val strings = Seq("abcde", "bbcdb", "aacbb", "baccc", "ddddd").toArray
    val data = (1 to 100).map(i => (i, strings(i % strings.length)))
    spark.createDataFrame(data).createOrReplaceTempView("t")
    sql("INSERT OVERWRITE TABLE oap_test SELECT * FROM t")

    // sql("SELECT * FROM oap_test").show()
    sql("CREATE OINDEX es_index ON oap_test (b) USING ES")
    // sql("SELECT * FROM oap_test WHERE b = 'abcde'").show()
    sql("SELECT * FROM oap_test WHERE b like '%ab%'").show()

    val file = new Path(path)
    val l = file.getFileSystem(new Configuration()).listStatus(file)

    val indexFiles = l.filter(f => f.getPath.getName.contains("es_index")).map(_.getPath.getName)
    println(indexFiles.mkString("\n"))
    // es.batchDelete(indexFiles, "all_oap_es_index")
    // println(es.queryAllRecordIds("all_oap_es_index").mkString(", "))

    // println(l.map(f => f.getPath.getName).mkString("\n"))

    /*
    es.batchDelete(indexFiles, "all_oap_es_index")

    es.dropESTableIndex("all_oap_es_index")
    sql("DROP OINDEX es_index ON oap_test")
    sqlContext.dropTempTable("oap_test")
    */
  }
  // scalastyle:on println

  /*
  test("index exists check") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    assert(es.checkEsTableIndexExists(TEST_TABLE_NAME))
    es.dropESTableIndex(TEST_TABLE_NAME)
    assert(!es.checkEsTableIndexExists(TEST_TABLE_NAME))
  }

  ignore("es table index create") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    assert(es.checkEsTableIndexExists(TEST_TABLE_NAME))
    es.dropESTableIndex(TEST_TABLE_NAME)
  }

  ignore("es table index drop") {
    es.createEsTableIndex(TEST_TABLE_NAME)
    es.dropESTableIndex(TEST_TABLE_NAME)
    assert(!es.checkEsTableIndexExists(TEST_TABLE_NAME))
  }

  ignore("insert records to es table") {
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

  ignore("query records ids by index file name") {
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

  ignore("drop records for oap index from es") {
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
  */
}
