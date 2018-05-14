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

package org.apache.spark.sql.execution.datasources.oap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeAndComment, CodeFormatter, CodeGenerator}
import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.types._

object GenerateOapColumnAccessor
  extends CodeGenerator[Seq[DataType], OapColumnarIterator] with Logging {

  override protected def canonicalize(in: Seq[DataType]): Seq[DataType] = in

  override protected def bind(in: Seq[DataType], inputSchema: Seq[Attribute]): Seq[DataType] = in

  override protected def create(columnTypes: Seq[DataType]): OapColumnarIterator = {

    val ctx = newCodeGenContext()
    val numFields = columnTypes.size

    val (initializeAccessors, extractors) = columnTypes.zipWithIndex.map { case (dt, index) =>

      val accessorName = ctx.freshName("accessor")

      val accessorCls = dt match {
        case BinaryType => classOf[BinaryColumnValues].getName
        case BooleanType => classOf[BooleanColumnValues].getName
        case ByteType => classOf[ByteColumnValues].getName
        case DateType => classOf[DateColumnValues].getName
        case DoubleType => classOf[DoubleColumnValues].getName
        case FloatType => classOf[FloatColumnValues].getName
        case IntegerType => classOf[IntColumnValues].getName
        case LongType => classOf[LongColumnValues].getName
        case ShortType => classOf[ShortColumnValues].getName
        case StringType => classOf[StringColumnValues].getName
        case other => throw new OapException(s"not support $other")
      }

      ctx.addMutableState(accessorCls, accessorName, "")

      val createCode = dt match {
        case t if ctx.isPrimitiveType(t) =>
          s"$accessorName = new $accessorCls(defaultSize, buffers[$index]);"
        case StringType | BinaryType =>
          s"$accessorName = new $accessorCls(defaultSize, buffers[$index]);"
        case other => throw new OapException(s"not support $other")
      }

      val extract = s"$accessorName.extractTo(mutableRow, currentRow, $index);"

      (createCode, extract)
    }.unzip

    val numberOfStatementsThreshold = 200
    val (initializerAccessorCalls, extractorCalls) =
      if (initializeAccessors.length <= numberOfStatementsThreshold) {
        (initializeAccessors.mkString("\n"), extractors.mkString("\n"))
      } else {
        throw new OapException("not support yet")
      }
    val codeBody = s"""
      |import scala.collection.Iterator;
      |import org.apache.hadoop.conf.Configuration;
      |import org.apache.spark.sql.catalyst.InternalRow;
      |import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
      |import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
      |import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
      |import org.apache.spark.sql.execution.columnar.MutableUnsafeRow;
      |import org.apache.spark.sql.execution.datasources.oap.ColumnValues;
      |import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCache;
      |import org.apache.spark.sql.execution.datasources.oap.filecache.FiberCacheManager;
      |import org.apache.spark.sql.execution.datasources.oap.filecache.DataFiber;
      |import org.apache.spark.sql.execution.datasources.oap.io.DataFile;
      |import org.apache.spark.sql.types.DataType;
      |import org.apache.spark.sql.types.LongType;
      |import org.apache.spark.sql.types.StructType;
      |
      |public SpecificColumnarIterator generate(Object[] references) {
      |  return new SpecificColumnarIterator();
      |}
      |
      |class SpecificColumnarIterator extends ${classOf[OapColumnarIterator].getName} {
      |
      |  private DataFile dataFile;
      |  private Configuration configuration;
      |
      |  private DataType[] columnTypes = null;
      |  private int[] columnIndexes = null;
      |  private FiberCache[] buffers = null;
      |
      |  private scala.collection.Iterator input = null;
      |
      |  private int numRowsInGroup = 0;
      |  private int currentGroupId = 0;
      |  private int currentRow = 0;
      |  private int defaultSize = 0;
      |
      |  private UnsafeRow unsafeRow = new UnsafeRow($numFields);
      |  private BufferHolder bufferHolder = new BufferHolder(unsafeRow);
      |  private UnsafeRowWriter rowWriter = new UnsafeRowWriter(bufferHolder, $numFields);
      |  private MutableUnsafeRow mutableRow = null;
      |
      |  ${ctx.declareMutableStates()}
      |
      |  public SpecificColumnarIterator() {
      |    this.buffers = new FiberCache[${columnTypes.length}];
      |    this.mutableRow = new MutableUnsafeRow(rowWriter);
      |  }
      |
      |  public void initialize(
      |      DataFile dataFile,
      |      Configuration configuration,
      |      Iterator input,
      |      DataType[] columnDataTypes,
      |      int[] columnIndexes,
      |      int defaultSize) {
      |
      |    this.dataFile = dataFile;
      |    this.configuration = configuration;
      |    this.input = input;
      |    this.columnTypes = columnTypes;
      |    this.columnIndexes = columnIndexes;
      |    this.defaultSize = defaultSize;
      |  }
      |
      |  public boolean hasNext() {
      |    if (currentRow < numRowsInGroup) {
      |      return true;
      |    }
      |    if (!input.hasNext()) {
      |      return false;
      |    }
      |
      |    ${classOf[RowGroupInfo].getName} group = (${classOf[RowGroupInfo].getName}) input.next();
      |
      |    currentRow = 0;
      |    currentGroupId = group.id();
      |    numRowsInGroup = group.rowCount();
      |
      |    for (int i = 0; i < columnIndexes.length; i ++) {
      |      if (buffers[i] != null) buffers[i].release();
      |      buffers[i] = FiberCacheManager.get(
      |        new DataFiber(dataFile, columnIndexes[i], currentGroupId), configuration);
      |    }
      |
      |    $initializerAccessorCalls
      |
      |    return hasNext();
      |  }
      |
      |  public InternalRow next() {
      |    bufferHolder.reset();
      |    rowWriter.zeroOutNullBytes();
      |    $extractorCalls
      |    unsafeRow.setTotalSize(bufferHolder.totalSize());
      |    currentRow += 1;
      |    return unsafeRow;
      |  }
      |
      |  public void close() {
      |    for (int i = 0; i < columnIndexes.length; i++) {
      |      if (buffers[i] != null) buffers[i].release();
      |    }
      |  }
      |}""".stripMargin

    val code = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(codeBody, ctx.getPlaceHolderToComments()))
    logDebug(s"Generated ColumnarIterator:\n${CodeFormatter.format(code)}")
    CodeGenerator.compile(code).generate(Array.empty).asInstanceOf[OapColumnarIterator]
  }
}
