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

import java.util.concurrent.Callable
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.google.common.cache._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging

trait OapCache {
  def get(fiber: Fiber, conf: Configuration): FiberCache
  def getDataFibers: mutable.Set[DataFiber]
  def getIndexFibers(indexName: String): mutable.Set[Fiber]
  def invalidate(fiber: Fiber): Unit
  def invalidateAll(fibers: Iterable[Fiber]): Unit
  def cacheSize: Long
  def cacheCount: Long
  // TODO: To be compatible with some test cases. But we shouldn't rely on Guava in trait.
  def cacheStats: CacheStats
}

class SimpleOapCache extends OapCache with Logging {

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    fiber.fiber2Data(conf)
  }

  override def getDataFibers: mutable.Set[DataFiber] = {
    mutable.Set.empty
  }

  override def getIndexFibers(indexName: String): mutable.Set[Fiber] = {
    mutable.Set.empty
  }

  override def invalidate(fiber: Fiber): Unit = {}

  override def invalidateAll(fibers: Iterable[Fiber]): Unit = {}

  override def cacheSize: Long = 0

  override def cacheStats: CacheStats = {
    new CacheStats(0, 0, 0, 0, 0, 0)
  }

  override def cacheCount: Long = 0
}

class GuavaOapCache(maxMemory: Long) extends OapCache with Logging {

  private val MB: Double = 1024 * 1024
  private val MAX_WEIGHT = (maxMemory / MB).toInt

  private val removalListener = new RemovalListener[Fiber, FiberCache] {
    override def onRemoval(notification: RemovalNotification[Fiber, FiberCache]): Unit = {
      // TODO: Change the log more readable
      logDebug(s"Removing Cache ${notification.getKey}")
      // TODO: Investigate lock mechanism to secure in-used FiberCache
      notification.getValue.dispose()
      _cacheSize.addAndGet(-notification.getValue.size())
    }
  }

  private val weigher = new Weigher[Fiber, FiberCache] {
    override def weigh(key: Fiber, value: FiberCache): Int =
      math.ceil(value.size() / MB).toInt
  }

  // Total cached size for debug purpose
  private val _cacheSize: AtomicLong = new AtomicLong(0)

  /**
   * To avoid storing configuration in each Cache, use a loader.
   * After all, configuration is not a part of Fiber.
   */
  private def cacheLoader(fiber: Fiber, configuration: Configuration) =
    new Callable[FiberCache] {
      override def call(): FiberCache = {
        logDebug(s"Loading Cache $fiber")
        val fiberCache = fiber.fiber2Data(configuration)
        _cacheSize.addAndGet(fiberCache.size())
        fiberCache
      }
    }

  private val cache = CacheBuilder.newBuilder()
      .recordStats()
      .concurrencyLevel(4)
      .removalListener(removalListener)
      .maximumWeight(MAX_WEIGHT)
      .weigher(weigher)
      .build[Fiber, FiberCache]()

  override def get(fiber: Fiber, conf: Configuration): FiberCache = {
    // Used a flag called disposed in FiberCache to indicate if this FiberCache is removed
    cache.get(fiber, cacheLoader(fiber, conf))
  }

  override def getDataFibers: mutable.Set[DataFiber] = {
    cache.asMap().keySet().asScala.collect {
      case fiber: DataFiber => fiber
    }
  }

  override def getIndexFibers(indexName: String): mutable.Set[Fiber] = {
    cache.asMap().keySet().asScala.filter {
      case BTreeFiber(_, file, _, _) => file.contains(indexName)
      case BitmapFiber(_, file, _, _) => file.contains(indexName)
      case _ => false
    }
  }

  override def invalidate(fiber: Fiber): Unit = {
    cache.invalidate(fiber)
  }

  override def invalidateAll(fibers: Iterable[Fiber]): Unit = {
    cache.invalidateAll(fibers.asJava)
  }

  override def cacheSize: Long = _cacheSize.get()

  override def cacheStats: CacheStats = cache.stats()

  override def cacheCount: Long = cache.size()
}
