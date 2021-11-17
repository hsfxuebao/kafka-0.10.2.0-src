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
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, ThreadFactory, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{Time, Utils}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
    def newThread(runnable: Runnable): Thread =
      Utils.newThread("executor-"+executorName, runnable, false)
  })

  // 延迟队列，按照失效时间排序
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 原子共享变量，所有时间轮公用一个计数器
  private[this] val taskCounter = new AtomicInteger(0)
  // 时间轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  // 延迟操作是一个定时任务类TimerTask
  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // 包装成定时任务条目
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 将定时任务条目加入时间轮，如果加入失败，会立即执行定时任务条目
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 添加到时间轮中
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 过期的任务立即执行
      if (!timerTaskEntry.cancelled)
        // 提交给线程池执行
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  // 重新加入
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  // 弹出超时的定时任务列表，将定时器的时钟往前移动，并将定时任务重新加入到定时器中
  def advanceClock(timeoutMs: Long): Boolean = {
    // 轮询队列的最长等待时间
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        // 延迟队列轮询出的桶，表示这个桶超时了
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration())
          // 重新将所有定时任务加入定时器，这样才有机会执行定时任务
          bucket.flush(reinsert)
          // 立即再轮询一次，如果没有超时，返回的桶为空
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown() {
    taskExecutor.shutdown()
  }

}

