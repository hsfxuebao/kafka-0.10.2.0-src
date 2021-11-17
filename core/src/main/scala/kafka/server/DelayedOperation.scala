/**
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

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 */
// 延迟的操作接口，定义了延迟操作对象的生命周期，从创建、尝试完成、完成、到超时
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

  // 在多线程环境下，原子变量控制只要一个线程能调用onComplete()方法
  private val completed = new AtomicBoolean(false)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  /**
   * 强制完成延迟的操作，如果没有完成，有两个方法会有机会调用这个方法
   * （1）在tryComplete()方法尝试完成时，判断到可以完成了，调用forceComplete()
   *  (2) 超时了，必须立即调用forceComplete()
   *  如果被当前线程完成，返回true,如果其他线程完成了，当前线程调用是返回false
   *
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel() // 取消定时器
      onComplete()
      true
    } else {
      false // 有其他线程完成了这个延迟的请求
    }
  }

  /**
   * Check if the delayed operation is already completed
   * 检查延迟的操作是否已经完成
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   * 当延迟的操作超时后会执行该回调，如果有多个线程，只有一个线程会执行一次
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   * 操作完成时的处理，在forceComplete() 中只会被调用一次
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   * 尝试完成被延迟的操作，如果可以完成，调用forceComplete()处理完成的逻辑
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete(). This can be overridden if the operation provides its
   * own synchronization.
   */
  def safeTryComplete(): Boolean = {
    synchronized {
      tryComplete()
    }
  }

  /*
   * run() method defines a task that is executed on timeout
   * 任务在超时时才会调用一次run()方法
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
// 保存延迟操作的延迟缓存，缓存的键是分区，值是延迟操作列表
// 延迟缓存的清理器、监视器、定时器之间都存在一定的关联关系
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  // 每个分区有多个延迟操作
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    var isCompletedByMe = operation.safeTryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted)
        return false
      // 将延迟操作加入监控
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true
        // 计数器，每个延迟操作只会增加一
        estimatedTotalOperations.incrementAndGet()
      }
    }

    isCompletedByMe = operation.safeTryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!operation.isCompleted) {
      // 将延迟任务加入定时器中
      timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = allWatchers.map(_.countWatched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
    // 在失效队列中被延迟的操作数量
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  // 所有的监视器
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   * 每个延迟缓存的键对应一个监视器，它管理了这个键的所有延迟操作
   */
  private class Watchers(val key: Any) { // 作为延迟缓存的内部类
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    // 将延迟操作对象加入到延迟缓存的监控中，它被延迟缓存的tryCompleteElseWatch()调用
    def watch(t: T) {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    // 尝试完成监视器中所有延迟操作，它被外部事件的checkAndComplete()调用
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          // 有其他线程完成了这个延迟操作
          iter.remove()
        } else if (curr.safeTryComplete()) {
          // 当前线程完成了这个延迟的操作
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    // traverse the list and purge elements that are already completed by others
    // 遍历列表，并移除已经完成的延迟操作，它被清理器周期性的调用
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    // 定时器时钟的滑动间隔，每隔200毫秒前进一次
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      // 清理监视器中已经完成的延迟操作
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  // 后台清理失效的延迟操作的线程
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    // 这只是一次运行的逻辑，线程的循坏运行定义在父类中
    override def doWork() {
      advanceClock(200L)
    }
  }
}
