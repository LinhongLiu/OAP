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

package org.apache.spark.sql.execution.datasources.spinach

import java.io.{ByteArrayInputStream, ObjectInputStream}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Ascending, JoinedRow, SortDirection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.execution.datasources.spinach.utils.IndexUtils
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet


private[spinach] object RangeScanner {
  val DUMMY_KEY_START: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
  val DUMMY_KEY_END: Key = InternalRow(Array[Any](): _*) // we compare the ref not the value
}

private[spinach] object CurrentKey {
  val INVALID_KEY_INDEX = -1
}


// B+ tree values in the leaf node, in long term, a single value should be associated
// with a single key, however, in order to eliminate the duplicated key in the B+ tree,
// we simply take out the values for the identical keys, and keep only a single key in the
// B+ tree leaf node
private[spinach] trait IndexNodeValue {
  def length: Int
  def apply(idx: Int): Long
}

// B+ Tree Node
private[spinach] trait IndexNode {
  def length: Int
  def keyAt(idx: Int): Key
  def childAt(idx: Int): IndexNode
  def valueAt(idx: Int): IndexNodeValue
  def next: IndexNode
  def isLeaf: Boolean
}

trait UnsafeIndexTree {
  def buffer: DataFiberCache
  def offset: Long
  def baseObj: Object = buffer.fiberData.getBaseObject
  def baseOffset: Long = buffer.fiberData.getBaseOffset
  def length: Int = Platform.getInt(baseObj, baseOffset + offset)
}

private[spinach] case class UnsafeIndexNodeValue(
    buffer: DataFiberCache,
    offset: Long,
    dataEnd: Long) extends IndexNodeValue with UnsafeIndexTree {
  // 4 <- value1, 8 <- value2
  override def apply(idx: Int): Long = Platform.getLong(baseObj, baseOffset + offset + 4 + idx * 8)

  // for debug
  private def values: Seq[Long] = (0 until length).map(apply)
  override def toString: String = "ValuesNode(" + values.mkString(",") + ")"
}

private[spinach] case class UnsafeIndexNode(
    buffer: DataFiberCache,
    offset: Long,
    dataEnd: Long,
    schema: StructType) extends IndexNode with UnsafeIndexTree {
  override def keyAt(idx: Int): Key = {
    // 16 <- value5, 12(4 + 8) <- value3 + value4
    val keyOffset = Platform.getLong(baseObj, baseOffset + offset + 12 + idx * 16)
    val len = Platform.getInt(baseObj, baseOffset + keyOffset)
//     val row = new UnsafeRow(schema.length) // this is for debug use
    val row = UnsafeIndexNode.row.get
    row.setNumFields(schema.length)
    row.pointTo(baseObj, baseOffset + keyOffset + 4, len)
    row
  }

  private def treeChildAt(idx: Int): UnsafeIndexTree = {
    // 16 <- value5, 20(4 + 8 + 8) <- value3 + value4 + value5/2
    val childOffset = Platform.getLong(baseObj, baseOffset + offset + 16 * idx + 20)
    if (isLeaf) {
      UnsafeIndexNodeValue(buffer, childOffset, dataEnd)
    } else {
      UnsafeIndexNode(buffer, childOffset, dataEnd, schema)
    }
  }

  override def childAt(idx: Int): UnsafeIndexNode =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNode]
  override def valueAt(idx: Int): UnsafeIndexNodeValue =
    treeChildAt(idx).asInstanceOf[UnsafeIndexNodeValue]
  // if the first child offset is in data segment (treeChildAt(0)), 20 <- 16 * 0 + 20
  override def isLeaf: Boolean = Platform.getLong(baseObj, baseOffset + offset + 20) < dataEnd
  override def next: UnsafeIndexNode = {
    // 4 <- value3
    val nextOffset = Platform.getLong(baseObj, baseOffset + offset + 4)
    if (nextOffset == -1L) {
      null
    } else {
      UnsafeIndexNode(buffer, nextOffset, dataEnd, schema)
    }
  }

  // for debug
  private def children: Seq[UnsafeIndexTree] = (0 until length).map(treeChildAt)
  private def keys: Seq[Key] = (0 until length).map(keyAt)
  override def toString: String =
    s"[Signs(${keys.map(_.getInt(0)).mkString(",")}) " + children.mkString(" ") + "]"
}

private[spinach] object UnsafeIndexNode {
  lazy val row = new ThreadLocal[UnsafeRow] {
    override def initialValue = new UnsafeRow
  }
}

private[spinach] class CurrentKey(node: IndexNode, keyIdx: Int, valueIdx: Int) {
  assert(node.isLeaf, "Should be Leaf Node")

  private var currentNode: IndexNode = node
  // currentKeyIdx is the flag that we check if we are in the end of the tree traversal
  private var currentKeyIdx: Int = if (node.length > keyIdx) {
    keyIdx
  } else {
    CurrentKey.INVALID_KEY_INDEX
  }

  private var currentValueIdx: Int = valueIdx

  private var currentValues: IndexNodeValue = if (currentKeyIdx != CurrentKey.INVALID_KEY_INDEX) {
    currentNode.valueAt(currentKeyIdx)
  } else {
    null
  }

  def currentKey: Key = if (currentKeyIdx == CurrentKey.INVALID_KEY_INDEX) {
    RangeScanner.DUMMY_KEY_END
  } else {
    currentNode.keyAt(currentKeyIdx)
  }

  def currentRowId: Long = currentValues(currentValueIdx)

  def moveNextValue: Unit = {
    if (currentValueIdx < currentValues.length - 1) {
      currentValueIdx += 1
    } else {
      moveNextKey
    }
  }

  def moveNextKey: Unit = {
    if (currentKeyIdx < currentNode.length - 1) {
      currentKeyIdx += 1
      currentValueIdx = 0
      currentValues = currentNode.valueAt(currentKeyIdx)
    } else {
      currentNode = currentNode.next
      if (currentNode != null) {
        currentKeyIdx = 0
        currentValueIdx = 0
        currentValues = currentNode.valueAt(currentKeyIdx)
      } else {
        currentKeyIdx = CurrentKey.INVALID_KEY_INDEX
      }
    }
  }

  def isEnd: Boolean = currentNode == null || (currentKey == RangeScanner.DUMMY_KEY_END)
}

// we scan the index from the smallest to the greatest, this is the root class
// of scanner, which will scan the B+ Tree (index) leaf node.
private[spinach] class RangeScanner(idxMeta: IndexMeta) extends Iterator[Long] with Serializable {
  // TODO this is a temp work around
  override def toString(): String = "RangeScanner"
//  @transient protected var currentKey: CurrentKey = _
  @transient protected var currentKeyArray: Array[CurrentKey] = _
  @transient protected var ordering: Ordering[Key] = _
  var intervalArray: ArrayBuffer[RangeInterval] = _
  protected var keySchema: StructType = _

  def meta: IndexMeta = idxMeta

  var currentKeyIdx = 0

  def exist(dataPath: Path, conf: Configuration): Boolean = {
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    path.getFileSystem(conf).exists(path)
  }


  def initialize(dataPath: Path, conf: Configuration): RangeScanner = {
    assert(keySchema ne null)
    // val root = BTreeIndexCacheManager(dataPath, context, keySchema, meta)
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, conf)
    val root = meta.open(indexData, keySchema)

    _init(root)
  }

  def _init(root : IndexNode): RangeScanner = {
    assert(intervalArray ne null, "intervalArray is null!")
    this.ordering = GenerateOrdering.create(keySchema)
    currentKeyArray = new Array[CurrentKey](intervalArray.length)
    currentKeyIdx = 0 // reset to initialized value for this thread
    intervalArray.zipWithIndex.foreach {
      case(interval: RangeInterval, i: Int) =>
        var order: Ordering[Key] = null
        if (interval.start == RangeScanner.DUMMY_KEY_START) {
          // find the first key in the left-most leaf node
          var tmpNode = root
          while (!tmpNode.isLeaf) tmpNode = tmpNode.childAt(0)
          currentKeyArray(i) = new CurrentKey(tmpNode, 0, 0)
        } else {
          // find the first identical key or the first key right greater than the specified one
          if (keySchema.size > interval.start.numFields) { // exists Dummy_Key
            order = GenerateOrdering.create(StructType(keySchema.dropRight(1)))
          } else order = this.ordering
          currentKeyArray(i) = moveTo(root, interval.start, true, order)
          if (keySchema.size > interval.end.numFields) { // exists Dummy_Key
            order = GenerateOrdering.create(StructType(keySchema.dropRight(1)))
            // find the last identical key or the last key less than the specified one on the left
            this.intervalArray(i).end = moveTo(root, interval.end, false, order).currentKey
          }

        }
        // process the LeftOpen condition
        while (!interval.startInclude &&
          currentKeyArray(i).currentKey != RangeScanner.DUMMY_KEY_END &&
          ordering.compare(interval.start, currentKeyArray(i).currentKey) == 0) {
          // find exactly the key, since it's LeftOpen, skip the equivalent key(s)
          currentKeyArray(i).moveNextKey
        }
    }
    this
  }

  // i: the interval index
  def intervalShouldStop(i: Int): Boolean = { // detect if we need to stop scanning
    if (intervalArray(i).end == RangeScanner.DUMMY_KEY_END) { // Left-Only search
      return false
    }
    if (intervalArray(i).endInclude) { // RightClose
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) > 0
    }
    else { // RightOpen
//      val k = currentKeyArray(i).currentKey
      ordering.compare(
        currentKeyArray(i).currentKey, intervalArray(i).end) >= 0
    }

  }

  /**
   * search the key that equals to candidate in the IndexNode of B+ tree
   * @param node: the node where binary search is executed
   * @param candidate: the candidate key
   * @param findFirst: indicates whether the goal is to find the
   *                  first appeared key that equals to candidate
   * @param order: the ordering that used to compare two keys
   * @return the CurrentKey object that points to the target key
   * findFirst == true -> find the first appeared key that equals to candidate, this is used
   * to determine the start key that begins the scan.
   * In this case, the first identical key(if found) or
   * the first key greater than the specified one on the right(if not found) is returned;
   * findFirst == false -> find the last appeared key that equals to candidate, this is used
   * to determine the end key that terminates the scan.
   * In this case, the last identical key(if found) or
   * the last key less than the specified one on the left(if not found) is returned.
   */
  protected def moveTo(node: IndexNode, candidate: Key, findFirst: Boolean, order: Ordering[Key])
  : CurrentKey = {
    var s = 0
    var e = node.length - 1
    var notFind = true

    var m = s
    while (s <= e & notFind) {
      m = (s + e) / 2
      val cmp = order.compare(node.keyAt(m), candidate)
      if (cmp == 0) {
        notFind = false
      } else if (cmp < 0) {
        s = m + 1
      } else {
        e = m - 1
      }
    }

    if (notFind) {
      m = if (e < 0) 0 else e
    }
    else { // the candidate key is found in the B+ tree
      if (findFirst) {// if the goal is to find the first appeared key that equals to candidate
        // if the goal is to find the start key,
        // then find the last key that is less than the specified one on the left
        // is always necessary in all Non-Leaf layers(considering the multi-column search)
        while (m>0 && order.compare(node.keyAt(m), candidate) == 0) {m -= 1}
        if (order.compare(node.keyAt(m), candidate) < 0) notFind = true
      } else {
        while (m<node.length-1 && order.compare(node.keyAt(m + 1), candidate) == 0)
          m += 1
      }
    }

    if (node.isLeaf) {
      // here currentKey is equal to candidate or the last key on the left side
      // which is less than the candidate
//      currentKeyArray(keyIdx) = new CurrentKey(node, m, 0)
      val currentKey = new CurrentKey(node, m, 0)

      if (notFind && findFirst) {
        // if not found and the goal is to find the start key, then let's move forward a key
        // if the goal is to find the end key, no need to move next
        if (order.compare(node.keyAt(m), candidate) < 0) {// if current key < candidate
//          currentKeyArray(keyIdx).moveNextKey
          currentKey.moveNextKey
        }
      }
      currentKey
    } else {
      moveTo(node.childAt(m), candidate, findFirst, order)
    }
  }

//  override def hasNext: Boolean = !(currentKey.isEnd || shouldStop(currentKey))
override def hasNext: Boolean = {
//  intervalArray.nonEmpty && !(currentKeyIdx == currentKeyArray.length-1 &&
//    (currentKeyArray(currentKeyIdx).isEnd || intervalShouldStop(currentKeyIdx)) )
  if (intervalArray.isEmpty) return false
  for(i <- currentKeyIdx until currentKeyArray.length) {
    if (!currentKeyArray(i).isEnd && !intervalShouldStop(i)) {
      return true
    }
  }// end for
  false
}

  override def next(): Long = {
    while (currentKeyArray(currentKeyIdx).isEnd || intervalShouldStop(currentKeyIdx)) {
      currentKeyIdx += 1
    }
    val rowId = currentKeyArray(currentKeyIdx).currentRowId
    currentKeyArray(currentKeyIdx).moveNextValue
    rowId
  }

  def withKeySchema(schema: StructType): RangeScanner = {
    this.keySchema = schema
    this
  }

}

private[spinach] case class BloomFilterScanner(me: IndexMeta) extends RangeScanner(me) {
  var stopFlag: Boolean = _

  var bloomFilter: BloomFilter = _

  var numOfElem: Int = _

  var curIdx: Int = _

  override def hasNext: Boolean = !stopFlag && curIdx < numOfElem

  override def next(): Long = {
    val tmp = curIdx
    curIdx += 1
    tmp.toLong
  }

  lazy val equalValues: Array[Key] = { // get equal value from intervalArray
    if (intervalArray.nonEmpty) {
      // should not use ordering.compare here
      intervalArray.filter(interval => (interval.start eq interval.end)
        && interval.startInclude && interval.endInclude).map(_.start).toArray
    } else null
  }

  override def initialize(inputPath: Path, configuration: Configuration): RangeScanner = {
    assert(keySchema ne null)
    this.ordering = GenerateOrdering.create(keySchema)

    val path = IndexUtils.indexFileFromDataFile(inputPath, meta.name)
    val indexScanner = IndexFiber(IndexFile(path))
    val indexData: IndexFiberCacheData = FiberCacheManager(indexScanner, configuration)

    def buffer: DataFiberCache = DataFiberCache(indexData.fiberData)
    def getBaseObj = buffer.fiberData.getBaseObject
    def getBaseOffset = buffer.fiberData.getBaseOffset
    val bitArrayLength = Platform.getInt(getBaseObj, getBaseOffset + 0 )
    val numOfHashFunc = Platform.getInt(getBaseObj, getBaseOffset + 4)
    numOfElem = Platform.getInt(getBaseObj, getBaseOffset + 8)

    var cur_pos = 4
    val bitSetLongArr = (0 until bitArrayLength).map( i => {
      cur_pos += 8
      Platform.getLong(getBaseObj, getBaseOffset + cur_pos)
    }).toArray

    bloomFilter = BloomFilter(bitSetLongArr, numOfHashFunc)

    // TODO need optimization while considering multi-column
    stopFlag = if (equalValues != null && equalValues.length > 0) {
      !equalValues.map(value => bloomFilter
        .checkExist(value.get(0, keySchema.head.dataType).toString))
        .reduceOption(_ || _).getOrElse(false)
    } else false
    curIdx = 0
    this
  }

  override def toString: String = "BloomFilterScanner"
}


// A dummy scanner will actually not do any scanning
private[spinach] object DUMMY_SCANNER extends RangeScanner(null) {
  //  override def shouldStop(key: CurrentKey): Boolean = true
  override def intervalShouldStop(i: Int): Boolean = true
  override def initialize(path: Path, configuration: Configuration): RangeScanner = { this }
  override def hasNext: Boolean = false
  override def next(): Long = throw new NoSuchElementException("end of iterating.")
//  override def withNewStart(key: Key, include: Boolean): RangeScanner = this
//  override def withNewEnd(key: Key, include: Boolean): RangeScanner = this
  override def meta: IndexMeta = throw new NotImplementedError()
//  override def start: Key = throw new NotImplementedError()
}

private[spinach] class FilterOptimizer(keySchema: StructType) {
  val order = GenerateOrdering.create(keySchema)

  // compare two intervals
  def compareRangeInterval(interval1: RangeInterval, interval2: RangeInterval): Boolean = {
    if ((interval1.start eq RangeScanner.DUMMY_KEY_START) &&
      (interval2.start ne RangeScanner.DUMMY_KEY_START)) {
      return true
    }
    if(interval2.start eq RangeScanner.DUMMY_KEY_START) {
      return false
    }
    order.compare(interval1.start, interval2.start) < 0
  }
  // unite interval extra to interval base
  // return: if two intervals is unioned
  def intervalUnion(base: RangeInterval, extra: RangeInterval): Boolean = {
    def union: Boolean = {// union two intervals
      if ((extra.end eq RangeScanner.DUMMY_KEY_END) || order.compare(extra.end, base.end)>0) {
        base.end = extra.end
        base.endInclude = extra.endInclude
        return true
      }
      if (order.compare(extra.end, base.end)==0) {
        base.endInclude = base.endInclude || extra.endInclude
      }
      true
    }// end def union

    if (base.start eq RangeScanner.DUMMY_KEY_START) {
      if (base.end eq RangeScanner.DUMMY_KEY_END) {
        return true
      }
      if (extra.start ne RangeScanner.DUMMY_KEY_START) {
        val cmp = order.compare(extra.start, base.end)
        if(cmp>0 || (cmp == 0 && !extra.startInclude && !base.endInclude)) {
          return false // cannot union
        }
      }
      // union two intervals
      union
    }
    else {// base.start is not DUMMY
      if (order.compare(extra.start, base.start)==0) {
        base.startInclude = base.startInclude || extra.startInclude
      }
      if (base.end eq RangeScanner.DUMMY_KEY_END) {
        return true
      }
      val cmp = order.compare(extra.start, base.end)
      if(cmp>0 || (cmp==0 && !extra.startInclude && !base.endInclude)) {
        return false // cannot union
      }
      // union two intervals
      union
    }
  }
  // Or operation: (union multiple range intervals which may overlap)
  def addBound(intervalArray1: ArrayBuffer[RangeInterval],
               intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    // firstly, put all intervals to intervalArray1
    intervalArray1 ++= intervalArray2
    if (intervalArray1.isEmpty) {
      return intervalArray1
    }

    val sortedArray = intervalArray1.sortWith(compareRangeInterval)

    val result = ArrayBuffer(sortedArray.head)
    for(i <- 1 until sortedArray.length) {
      val interval = result.last
      if ((interval.end eq RangeScanner.DUMMY_KEY_END) && interval.startInclude) {
        return result
      }
      if(!intervalUnion(interval, sortedArray(i))) {
        result += sortedArray(i)
      }

    }// end for
    result
  }

  // merge two key and their include identifiers
  def intersect(key1: Key, key2: Key, include1: Boolean, include2: Boolean,
                isEndKey: Boolean): (Key, Boolean) = {
    if (key1 == RangeScanner.DUMMY_KEY_START) {
      (key2, include2)
    }
    else {
      if (key2 == RangeScanner.DUMMY_KEY_START) {
        (key1, include1)
      }
      else { // both key1 and key2 are not Dummy
        if (order.compare(key1, key2) == 0) {
          return (key1, include1 && include2)
        }
        if (order.compare(key1, key2) > 0 ^ isEndKey) {
          (key1, include1)
        }
        else {
          (key2, include2)
        }
      }
    }
  }

  // verify non-empty intervals
  def validate(interval: RangeInterval): Boolean = {
    if ((interval.start ne RangeScanner.DUMMY_KEY_START)
      && (interval.end ne RangeScanner.DUMMY_KEY_END)) {
      if (order.compare(interval.start, interval.end)>0) {
        return false
      }
      if (order.compare(interval.start, interval.end) == 0
        && (!interval.startInclude || !interval.endInclude)) {
        return false
      }
    }
    true
  }

  // And operation: (intersect multiple range intervals)
  def mergeBound(intervalArray1: ArrayBuffer[RangeInterval],
                 intervalArray2: ArrayBuffer[RangeInterval] ): ArrayBuffer[RangeInterval] = {
    val intervalArray = for {
      interval1 <- intervalArray1
      interval2 <- intervalArray2
    } yield {
      val interval = new RangeInterval(
        RangeScanner.DUMMY_KEY_START, RangeScanner.DUMMY_KEY_END, true, true)

      val re1 = intersect(interval1.start, interval2.start,
        interval1.startInclude, interval2.startInclude, false)
      interval.start = re1._1
      interval.startInclude = re1._2

      val re2 = intersect(interval1.end, interval2.end,
        interval1.endInclude, interval2.endInclude, true)
      interval.end = re2._1
      interval.endInclude = re2._2
      interval
    }
    // retain non-empty intervals
    intervalArray.filter(validate)
  }
}

private[spinach] class IndexContext(meta: DataSourceMeta) {
  // availableIndexes keeps the available indexes for the current SQL query statement
  // (Int, IndexMeta):
  // if indexType is BloomFilter, then the Int represents the indice of the Index entries;
  // if indexType is B+Tree and single column,
  // then the Int represents the indice of the Index entries, that is 0;
  // if indexType is B+Tree and multi-column,
  // then the Int represents the last matched column indice of the Index entries
  private val availableIndexes = new mutable.ArrayBuffer[(Int, IndexMeta)]()
  private val filterMap = new mutable.HashMap[String, FilterOptimizer]()
  private var scanner: RangeScanner = _

  def getScanner: Option[RangeScanner] = Option(scanner)

  def selectAvailableIndex(intervalMap: mutable.HashMap[String, ArrayBuffer[RangeInterval]])
  : Unit = {
    var idx = 0
    while (idx < meta.indexMetas.length) {
      meta.indexMetas(idx).indexType match {
        case BTreeIndex(entries) if entries.length == 1 =>
          val attribute = meta.schema(entries(0).ordinal).name
          if (intervalMap.contains(attribute)) {
            availableIndexes.append((0, meta.indexMetas(idx)) )
          }
        case BTreeIndex(entries) =>
          var num = 0 // the number of matched column
          var flag = 0
          // flag (terminated indication):
          // 0 -> Equivalence column; 1 -> Range column; 2 -> Absent column
          for (entry <- entries if flag == 0) {
            val attribute = meta.schema(entry.ordinal).name
            if (intervalMap.contains(attribute) && intervalMap(attribute).length == 1) {
              val start = intervalMap(attribute).head.start
              val end = intervalMap(attribute).head.end
              val ordering = unapply(attribute).get.order
              if(start != RangeScanner.DUMMY_KEY_START &&
                end != RangeScanner.DUMMY_KEY_END &&
                ordering.compare(start, end) == 0) {num += 1} else flag = 1
            }
            else {
              if (!intervalMap.contains(attribute)) flag = 2 else flag = 1
            }
          } // end for
          if (flag == 1) num += 1
          if (num>0) {
            availableIndexes.append( (num-1, meta.indexMetas(idx)) )
          }
        case BloomFilterIndex(entries) =>
          // traverse all attributes that are in the bloomIndex,
          // return indexMeta and the first one's index which matches
          // TODO support multiple key in the index
          var flag = true
          var attrName: String = null
          for (entry <- entries if flag) {
            attrName = meta.schema(entry).name
            if (intervalMap.contains(attrName) &&
              intervalMap(attrName).length == 1) {
              val ordering = unapply(attrName).get.order
              val start = intervalMap(attrName).head.start
              val end = intervalMap(attrName).head.end
              if(start != RangeScanner.DUMMY_KEY_START &&
                end != RangeScanner.DUMMY_KEY_END && ordering.compare(start, end) == 0) {
                availableIndexes.append((entries.indexOf(entry), meta.indexMetas(idx)))
                flag = false
              }
            }
          }
        case BitMapIndex(entries) =>
          for (entry <- entries) {
            if (intervalMap.contains(meta.schema(entry).name)) {
              availableIndexes.append((entries.indexOf(entry), meta.indexMetas(idx)) )
            }
          }
        case other => // TODO support other types of index
      }
      idx += 1
    } // end while
  }

  /**
   * A simple approach to select best indexer:
   * For B+ tree index, we expect to make full use of index:
   * On one hand, match as many attributes as possible in a SQL statement;
   * On the other hand, use as many attributes as possible in a B+ tree index
   * So we want the number of matched attributes to be close to
   * both the total number of attributes in a SQL statement
   * and the total number of entries in a B+ tree index candidate
   * we introduce a variable ratio to indicate the match extent
   * ratio = totalAttributes/matchedAttributed + totalIndexEntries/matchedAttributes
   * @param attrNum: the total number of attributes in the SQL statement
   * @return (Int, IndexMeta): the best indexMeta,
   *         and the Int is the index of the last matched attribute in the index entries
   */
  def getBestIndexer(attrNum: Int): (Int, IndexMeta) = {
    var lastIdx = -1
    var bestIndexer: IndexMeta = null
    var ratio: Double = 0.0
    var isFirst = true
    for ((idx, indexMeta) <- availableIndexes) {
      indexMeta.indexType match {
        case BTreeIndex(entries) =>
          val matchedAttr: Double = idx + 1
          val currentRatio = attrNum/matchedAttr + entries.length/matchedAttr
          if (isFirst || ratio > currentRatio) {
            ratio = currentRatio
            bestIndexer = indexMeta
            lastIdx = idx
            isFirst = false
          }
        case _ =>
      }
    }
    if (bestIndexer == null && availableIndexes.nonEmpty) {
      lastIdx = availableIndexes.head._1
      bestIndexer = availableIndexes.head._2
    }
    (lastIdx, bestIndexer)
  }

  def buildScanner(lastIdx: Int, bestIndexer: IndexMeta, intervalMap:
  mutable.HashMap[String, ArrayBuffer[RangeInterval]]): Unit = {
//    intervalArray.sortWith(compare)
    if (lastIdx == -1 && bestIndexer == null) return
    var keySchema: StructType = null
    bestIndexer.indexType match {
      case BTreeIndex(entries) if entries.length == 1 =>
        keySchema = new StructType().add(meta.schema(entries(lastIdx).ordinal))
        scanner = new RangeScanner(bestIndexer)
        val attribute = meta.schema(entries(lastIdx).ordinal).name
        val filterOptimizer = unapply(attribute).get
        scanner.intervalArray =
          intervalMap(attribute).sortWith(filterOptimizer.compareRangeInterval)
      case BTreeIndex(entries) =>
        val indexFields = for (idx <- entries.map(_.ordinal)) yield meta.schema(idx)
        val fields = indexFields.slice(0, lastIdx + 1)
        keySchema = StructType(fields)
        scanner = new RangeScanner(bestIndexer)
        val attributes = fields.map(_.name) // get column names in the composite index
        scanner.intervalArray = new ArrayBuffer[RangeInterval](intervalMap(attributes.last).length)

        for (i <- intervalMap(attributes.last).indices) {
          val startKeys = attributes.indices.map(attrIdx =>
            if (attrIdx == attributes.length-1) intervalMap(attributes(attrIdx))(i).start
            else intervalMap(attributes(attrIdx)).head.start )
          val compositeStartKey = startKeys.reduce((key1, key2) => new JoinedRow(key1, key2))

          val endKeys = attributes.indices.map(attrIdx =>
            if (attrIdx == attributes.length-1) intervalMap(attributes(attrIdx))(i).end
            else intervalMap(attributes(attrIdx)).head.end )
          val compositeEndKey = endKeys.reduce((key1, key2) => new JoinedRow(key1, key2))

          scanner.intervalArray.append(
            RangeInterval(compositeStartKey, compositeEndKey,
            intervalMap(attributes.last)(i).startInclude,
            intervalMap(attributes.last)(i).endInclude)
          )

        } // end for
      case BloomFilterIndex(entries) =>
        keySchema = new StructType().add(meta.schema(entries(lastIdx)))
        scanner = BloomFilterScanner(bestIndexer)
        val attribute = meta.schema(entries(lastIdx)).name
        val filterOptimizer = unapply(attribute).get
        scanner.intervalArray =
          intervalMap(attribute).sortWith(filterOptimizer.compareRangeInterval)
      case BitMapIndex(entries) =>
        keySchema = new StructType().add(meta.schema(entries(lastIdx)))
        scanner = BitMapScanner(bestIndexer)
        val attribute = meta.schema(entries(lastIdx)).name
        val filterOptimizer = unapply(attribute).get
        scanner.intervalArray =
          intervalMap(attribute).sortWith(filterOptimizer.compareRangeInterval)
      case _ =>
    }

    scanner.withKeySchema(keySchema)
  }

  def unapply(attribute: String): Option[FilterOptimizer] = {
    if (!filterMap.contains(attribute)) {
      val ordinal = meta.schema.fieldIndex(attribute)
      filterMap.put(attribute, new FilterOptimizer(new StructType().add(meta.schema(ordinal))))
    }
    filterMap.get(attribute)
  }

  def unapply(value: Any): Option[Key] =
    Some(InternalRow(CatalystTypeConverters.convertToCatalyst(value)))
}

private[spinach] object DummyIndexContext extends IndexContext(null) {
  override def getScanner: Option[RangeScanner] = None
  override def unapply(attribute: String): Option[FilterOptimizer] = None
  override def unapply(value: Any): Option[Key] = None
}

private [spinach] class RangeInterval(s: Key, e: Key, includeStart: Boolean, includeEnd: Boolean)
  extends Serializable{
  var start = s
  var end = e
  var startInclude = includeStart
  var endInclude = includeEnd
}
private [spinach] object RangeInterval{
  def apply(s: Key, e: Key, includeStart: Boolean, includeEnd: Boolean): RangeInterval
  = new RangeInterval(s, e, includeStart, includeEnd)
}

// The build the BPlushTree Search Scanner according to the filter and indices,
private[spinach] object BPlusTreeSearch extends Logging {
  def optimizeFilterBound(filter: Filter, ic: IndexContext)
  : mutable.HashMap[String, ArrayBuffer[RangeInterval]] = {
    filter match {
      case And(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        for((attribute, intervals) <- rightMap) {
          if (leftMap.contains(attribute)) {
            attribute match {
            case ic (filterOptimizer) => // extract the corresponding scannerBuilder
              // combine all intervals of the same attribute of leftMap and rightMap
            leftMap.put(attribute,
              filterOptimizer.mergeBound(leftMap.getOrElseUpdate (attribute, null), intervals) )
            case _ => // this attribute does not exist, do nothing
            }
          }
          else {
            leftMap.put(attribute, intervals)
          }
        }// end for
        // rightMap.clear()
        leftMap
      case Or(leftFilter, rightFilter) =>
        val leftMap = optimizeFilterBound(leftFilter, ic)
        val rightMap = optimizeFilterBound(rightFilter, ic)
        for((attribute, intervals) <- rightMap) {
          if (leftMap.contains(attribute)) {
            attribute match {
            case ic (filterOptimizer) => // extract the corresponding scannerBuilder
              // add bound of the same attribute to the left map
              leftMap.put(attribute,
                filterOptimizer.addBound(leftMap.getOrElse (attribute, null), intervals) )
            case _ => // this attribute does not exist, do nothing
            }
          }
          else {
            leftMap.put(attribute, intervals)
          }

        }// end for
//        rightMap.clear()
        leftMap

      case EqualTo(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, key, true, true)
        scala.collection.mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, RangeScanner.DUMMY_KEY_END, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case GreaterThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(key, RangeScanner.DUMMY_KEY_END, false, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThanOrEqual(attribute, ic(key)) =>
        val ranger = new RangeInterval(RangeScanner.DUMMY_KEY_START, key, true, true)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case LessThan(attribute, ic(key)) =>
        val ranger = new RangeInterval(RangeScanner.DUMMY_KEY_START, key, true, false)
        mutable.HashMap(attribute -> ArrayBuffer(ranger))
      case _ => null
    }
  }

  // return whether a Filter predicate can be supported by our current work
  def canSupport(filter: Filter, ic: IndexContext): Boolean = {
    filter match {
      case EqualTo(ic(indexer), _) => true
      case GreaterThan(ic(indexer), _) => true
      case GreaterThanOrEqual(ic(indexer), _) => true
      case LessThan(ic(indexer), _) => true
      case LessThanOrEqual(ic(indexer), _) => true
      case Or(ic(indexer), _) => true
      case And(ic(indexer), _) => true
      case _ => false
    }
  }

  def build(filters: Array[Filter], ic: IndexContext): Array[Filter] = {
    if (filters == null || filters.isEmpty) return filters
    val intervalMapArray = filters.map(optimizeFilterBound(_, ic))
    // reduce multiple hashMap to one hashMap(AND operation)
    val intervalMap = intervalMapArray.reduce(
      (leftMap, rightMap) => {
        if (leftMap == null || leftMap.isEmpty) {
          rightMap
        }
        else if (rightMap == null || rightMap.isEmpty) {
          leftMap
        }
        else {
          for ((attribute, intervals) <- rightMap) {
            if (leftMap.contains(attribute)) {
              attribute match {
                case ic (filterOptimizer) => // extract the corresponding scannerBuilder
                // combine all intervals of the same attribute of leftMap and rightMap
                  leftMap.put(attribute,
                filterOptimizer.mergeBound(leftMap.getOrElseUpdate (attribute, null), intervals) )
                case _ => // this attribute is not index, do nothing
              }
            }
            else {
              leftMap.put(attribute, intervals)
            }
          } // end for
          // rightMap.clear()
          leftMap
        }
      }
    )

    if (intervalMap != null) {
      ic.selectAvailableIndex(intervalMap)
      val (num, idxMeta) = ic.getBestIndexer(intervalMap.size)
      ic.buildScanner(num, idxMeta, intervalMap)
    }

    filters.filterNot(canSupport(_, ic))
  }

}
