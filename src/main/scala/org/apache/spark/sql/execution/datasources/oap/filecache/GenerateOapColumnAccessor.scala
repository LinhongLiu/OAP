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

package org.apache.spark.sql.execution.datasources.oap.filecache

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.columnar.ColumnarIterator
import org.apache.spark.sql.execution.columnar.GenerateColumnAccessor.newCodeGenContext
import org.apache.spark.sql.types.DataType

object GenerateOapColumnAccessor
  extends CodeGenerator[Seq[DataType], OapColumnarIterator] with Logging {

  override protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in

  override protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  override protected def create(columnTypes: Seq[DataType]): OapColumnarIterator = {

    val ctx = newCodeGenContext()
    val numFields = columnTypes.size

    val (initializeAccessors, extractors) = columnTypes.zipWithIndex.map { case (dt, index) =>

      val accessorName = ctx.freshName("accessor")


      val extract = s"$accessorName.extractTo(mutableRow, $index);"

      ("123", "456")
    }
  }
}
