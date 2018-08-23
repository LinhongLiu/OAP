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

import org.elasticsearch.common.xcontent.XContentFactory

object OapEsProperties {
  val ES_INDEX_NAME = "all_oap_es_index"
  val DOC_TYPE = "oap_records"
  val VALUE_FIELD = "value"
  val ROW_ID_FIELD = "row_id"
  val INDEX_FILE_NAME_FIELD = "index_file_name"

  // TODO: Change this to a Map[String, Any]
  val mappings = XContentFactory.jsonBuilder()
    .startObject()
    // Settings of this Index
    .startObject("settings")
    .field("number_of_shards", 1)
    .field("number_of_replicas", 0)
    .endObject()
    .startObject("mappings")
    // This is the document type, called "oap_records"
    .startObject(DOC_TYPE)
    .startObject("properties")
    // value in indexed column.
    .startObject(VALUE_FIELD)
    .field("type", "text")
    .field("store", "yes")
    .field("index", "analyzed")
    .endObject()
    // index file name = data file name + index name
    .startObject(INDEX_FILE_NAME_FIELD)
    .field("type", "string")
    .field("store", "yes")
    .field("index", "not_analyzed")
    .endObject()
    // The row id of this record in data file
    .startObject(ROW_ID_FIELD)
    .field("type", "integer")
    .field("store", "yes")
    .endObject()
    .endObject()
    .endObject()
    .endObject()
    .endObject()
}
