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

package kafka.log

import java.io._
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import kafka.utils._

import scala.collection._
import scala.collection.JavaConverters._
import kafka.common.{KafkaException, KafkaStorageException}
import kafka.server.{BrokerState, OffsetCheckpoint, RecoveringFromUncleanShutdown}
import java.util.concurrent.{ExecutionException, ExecutorService, Executors, Future}

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 */
// log.dirs 数据目录 logDirs日志目录 日志管理器
@threadsafe
class LogManager(val logDirs: Array[File],
                 val topicConfigs: Map[String, LogConfig],
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 time: Time) extends Logging {
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  val InitialTaskDelayMs = 30*1000

  private val logCreationOrDeletionLock = new Object
  // 可以得出一个结论 一个分区（磁盘上的一个目录）对应一个log对象
  private val logs = new Pool[TopicPartition, Log]()
  private val logsToBeDeleted = new LinkedBlockingQueue[Log]()

  // todo 这个代码肯定被执行
  createAndValidateLogDirs(logDirs)
  private val dirLocks = lockLogDirs(logDirs)
  // 每个数据目录都有一个检查点文件，存储这个数据目录下所有分区的检查点信息
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  // todo 这个代码肯定被执行 创建日志管理类，就会立即调用该方法，加载所有的日志
  loadLogs()

  // public, so we can access this from kafka.admin.DeleteTopicTest
  val cleaner: LogCleaner =
    if(cleanerConfig.enableCleaner)
      new LogCleaner(cleanerConfig, logDirs, logs, time = time)
    else
      null
  
  /**
   * Create and check validity of the given directories, specifically:
   * <ol>
   * <li> Ensure that there are no duplicates in the directory list
   * <li> Create each directory if it doesn't exist
   * <li> Check that each path is a readable directory 
   * </ol>
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    // 判断是否有重复的目录
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    // 遍历所有配置的目录
    for(dir <- dirs) {
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        // 如果代码第一次进来，就需要创建好所有的目录
        val created = dir.mkdirs()
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      val lock = new FileLock(new File(dir, LockFile))
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recover and load all logs in the given data directories
   */
  private def loadLogs(): Unit = {
    info("Loading logs.")
    val startMs = time.milliseconds
    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // 遍历所有的目录（配置的存储日志的目录）
    for (dir <- this.logDirs) {
      // 为每个目录都创建一个线程池
      // 后面肯定是启动线程池里面去加载Log
      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val cleanShutdownFile = new File(dir, Log.CleanShutdownFile)

      if (cleanShutdownFile.exists) {
        debug(
          "Found clean shutdown file. " +
          "Skipping recovery for all logs in data directory: " +
          dir.getAbsolutePath)
      } else {
        // log recovery itself is being performed by `Log` class during initialization
        brokerState.newState(RecoveringFromUncleanShutdown)
      }

      var recoveryPoints = Map[TopicPartition, Long]()
      try {
        recoveryPoints = this.recoveryPointCheckpoints(dir).read
      } catch {
        case e: Exception =>
          warn("Error occured while reading recovery-point-offset-checkpoint file of directory " + dir, e)
          warn("Resetting the recovery checkpoint to 0")
      }

      val jobsForDir = for {
        dirContent <- Option(dir.listFiles).toList
        // TODO logDir 表示一个分区的目录
        logDir <- dirContent if logDir.isDirectory
      } yield {
        CoreUtils.runnable {
          debug("Loading log '" + logDir.getName + "'")

          // todo  获取分区信息
          val topicPartition = Log.parseTopicPartitionName(logDir)
          val config = topicConfigs.getOrElse(topicPartition.topic, defaultConfig)
          val logRecoveryPoint = recoveryPoints.getOrElse(topicPartition, 0L)

          // todo 创建log对象
          val current = new Log(logDir, config, logRecoveryPoint, scheduler, time)
          if (logDir.getName.endsWith(Log.DeleteDirSuffix)) {
            this.logsToBeDeleted.add(current)
          } else {
            // todo 把Log对象放入到logs里面，日志管理的映射表
            val previous = this.logs.put(topicPartition, current)
            if (previous != null) {
              throw new IllegalArgumentException(
                "Duplicate log directories found: %s, %s!".format(
                  current.dir.getAbsolutePath, previous.dir.getAbsolutePath))
            }
          }
        }
      }
      // 提交任务
      jobs(cleanShutdownFile) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((cleanShutdownFile, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)
        cleanShutdownFile.delete()
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during logs loading: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
    }

    info(s"Logs loading complete in ${time.milliseconds - startMs} ms.")
  }

  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    // 定时调度四个任务
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      // todo 定时检查文件 清理超时的文件
      scheduler.schedule("kafka-log-retention",
                         cleanupLogs,
                         delay = InitialTaskDelayMs,
                         period = retentionCheckMs,
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      // todo 定时把内存里面的数据刷写到磁盘
      scheduler.schedule("kafka-log-flusher", 
                         flushDirtyLogs, 
                         delay = InitialTaskDelayMs, 
                         period = flushCheckMs, 
                         TimeUnit.MILLISECONDS)

      /**
       * 定时更新一个检查点的文件
       * kafka服务有时候会涉及重启
       * 重启之后需要恢复那些数据？ka
       * 在这里会更新一个检查点文件-> 服务于kafka重启时恢复数据使用
       */
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs,
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-delete-logs",
                         deleteLogs,
                         delay = InitialTaskDelayMs,
                         period = defaultConfig.fileDeleteDelayMs,
                         TimeUnit.MILLISECONDS)
    }
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    info("Shutting down.")

    val threadPools = mutable.ArrayBuffer.empty[ExecutorService]
    val jobs = mutable.Map.empty[File, Seq[Future[_]]]

    // stop the cleaner first
    if (cleaner != null) {
      CoreUtils.swallow(cleaner.shutdown())
    }

    // close logs in each dir
    for (dir <- this.logDirs) {
      debug("Flushing and closing logs at " + dir)

      val pool = Executors.newFixedThreadPool(ioThreads)
      threadPools.append(pool)

      val logsInDir = logsByDir.getOrElse(dir.toString, Map()).values

      val jobsForDir = logsInDir map { log =>
        CoreUtils.runnable {
          // flush the log to ensure latest possible recovery point
          log.flush()
          log.close()
        }
      }

      jobs(dir) = jobsForDir.map(pool.submit).toSeq
    }


    try {
      for ((dir, dirJobs) <- jobs) {
        dirJobs.foreach(_.get)

        // update the last flush point
        debug("Updating recovery points at " + dir)
        checkpointLogsInDir(dir)

        // mark that the shutdown was clean by creating marker file
        debug("Writing clean shutdown marker at " + dir)
        CoreUtils.swallow(new File(dir, Log.CleanShutdownFile).createNewFile())
      }
    } catch {
      case e: ExecutionException => {
        error("There was an error in one of the threads during LogManager shutdown: " + e.getCause)
        throw e.getCause
      }
    } finally {
      threadPools.foreach(_.shutdown())
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }

    info("Shutdown complete.")
  }


  /**
   * Truncate the partition logs to the specified offsets and checkpoint the recovery point to this offset
   *
   * @param partitionOffsets Partition logs that need to be truncated
   */
  def truncateTo(partitionOffsets: Map[TopicPartition, Long]) {
    for ((topicPartition, truncateOffset) <- partitionOffsets) {
      val log = logs.get(topicPartition)
      // If the log does not exist, skip it
      if (log != null) {
        //May need to abort and pause the cleaning of the log, and resume after truncation is done.
        val needToStopCleaner: Boolean = truncateOffset < log.activeSegment.baseOffset
        if (needToStopCleaner && cleaner != null)
          cleaner.abortAndPauseCleaning(topicPartition)
        log.truncateTo(truncateOffset)
        if (needToStopCleaner && cleaner != null) {
          cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
          cleaner.resumeCleaning(topicPartition)
        }
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   *  Delete all data in a partition and start the log at the new offset
   *  @param newOffset The new offset to start the log with
   */
  def truncateFullyAndStartAt(topicPartition: TopicPartition, newOffset: Long) {
    val log = logs.get(topicPartition)
    // If the log does not exist, skip it
    if (log != null) {
        //Abort and pause the cleaning of the log, and resume after truncation is done.
      if (cleaner != null)
        cleaner.abortAndPauseCleaning(topicPartition)
      log.truncateFullyAndStartAt(newOffset)
      if (cleaner != null) {
        cleaner.maybeTruncateCheckpoint(log.dir.getParentFile, topicPartition, log.activeSegment.baseOffset)
        cleaner.resumeCleaning(topicPartition)
      }
    }
    checkpointRecoveryPointOffsets()
  }

  /**
   * Write out the current recovery point for all logs to a text file in the log directory 
   * to avoid recovering the whole log on startup.
   * 通常所有数据目录都会一起执行，不会专门操作某一个数据目录的检查点文件
   */
  def checkpointRecoveryPointOffsets() {
    // 数据目录下每个分区对应一个日志目录
    this.logDirs.foreach(checkpointLogsInDir)
  }

  /**
   * Make a checkpoint for all logs in provided directory.
   * 对数据目录下的所有日志（即所有分区），将其检查点写入到检查点文件
   */
  private def checkpointLogsInDir(dir: File): Unit = {
    val recoveryPoints = this.logsByDir.get(dir.toString)
    if (recoveryPoints.isDefined) {
      this.recoveryPointCheckpoints(dir).write(recoveryPoints.get.mapValues(_.recoveryPoint))
    }
  }

  /**
   * Get the log if it exists, otherwise return None
   */
  def getLog(topicPartition: TopicPartition): Option[Log] = Option(logs.get(topicPartition))

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   * 根据分区编号创建一个日志实例，并加入映射关系中
   */
  def createLog(topicPartition: TopicPartition, config: LogConfig): Log = {
    logCreationOrDeletionLock synchronized {
      // create the log if it has not already been created in another thread
      getLog(topicPartition).getOrElse {
        // 选择一个数据目录存储日志
        val dataDir = nextLogDir()
        val dir = new File(dataDir, topicPartition.topic + "-" + topicPartition.partition)
        // 日志目录的名称由主题和分区组成
        dir.mkdirs()
        val log = new Log(dir, config, recoveryPoint = 0L, scheduler, time)
        logs.put(topicPartition, log)
        info("Created log for partition [%s,%d] in %s with properties {%s}."
          .format(topicPartition.topic,
            topicPartition.partition,
            dataDir.getAbsolutePath,
            config.originals.asScala.mkString(", ")))
        log
      }
    }
  }

  /**
   *  Delete logs marked for deletion.
   */
  private def deleteLogs(): Unit = {
    try {
      var failed = 0
      while (!logsToBeDeleted.isEmpty && failed < logsToBeDeleted.size()) {
        val removedLog = logsToBeDeleted.take()
        if (removedLog != null) {
          try {
            removedLog.delete()
            info(s"Deleted log for partition ${removedLog.topicPartition} in ${removedLog.dir.getAbsolutePath}.")
          } catch {
            case e: Throwable =>
              error(s"Exception in deleting $removedLog. Moving it to the end of the queue.", e)
              failed = failed + 1
              logsToBeDeleted.put(removedLog)
          }
        }
      }
    } catch {
      case e: Throwable => 
        error(s"Exception in kafka-delete-logs thread.", e)
    }
}

  /**
    * Rename the directory of the given topic-partition "logdir" as "logdir.uuid.delete" and 
    * add it in the queue for deletion. 
    * @param topicPartition TopicPartition that needs to be deleted
    */
  def asyncDelete(topicPartition: TopicPartition) = {
    val removedLog: Log = logCreationOrDeletionLock synchronized {
                            logs.remove(topicPartition)
                          }
    if (removedLog != null) {
      //We need to wait until there is no more cleaning task on the log to be deleted before actually deleting it.
      if (cleaner != null) {
        cleaner.abortCleaning(topicPartition)
        cleaner.updateCheckpoints(removedLog.dir.getParentFile)
      }
      // renaming the directory to topic-partition.uniqueId-delete
      val dirName = new StringBuilder(removedLog.name)
                        .append(".")
                        .append(java.util.UUID.randomUUID.toString.replaceAll("-",""))
                        .append(Log.DeleteDirSuffix)
                        .toString()
      removedLog.close()
      val renamedDir = new File(removedLog.dir.getParent, dirName)
      val renameSuccessful = removedLog.dir.renameTo(renamedDir)
      if (renameSuccessful) {
        removedLog.dir = renamedDir
        // change the file pointers for log and index file
        for (logSegment <- removedLog.logSegments) {
          logSegment.log.setFile(new File(renamedDir, logSegment.log.file.getName))
          logSegment.index.file = new File(renamedDir, logSegment.index.file.getName)
        }

        logsToBeDeleted.add(removedLog)
        removedLog.removeLogMetrics()
        info(s"Log for partition ${removedLog.topicPartition} is renamed to ${removedLog.dir.getAbsolutePath} and is scheduled for deletion")
      } else {
        throw new KafkaStorageException("Failed to rename log directory from " + removedLog.dir.getAbsolutePath + " to " + renamedDir.getAbsolutePath)
      }
    }
  }

  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      var dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
    }
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   * Only consider logs that are not compacted.
   * 日志管理器的日志清理任务
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    var total = 0
    val startMs = time.milliseconds
    // 遍历所有的log
    for(log <- allLogs; if !log.config.compact) {
      debug("Garbage collecting '" + log.name + "'")
      // 删除满足条件的文件
      total += log.deleteOldSegments()
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

  /**
   * Get a map of TopicPartition => Log
   */
  def logsByTopicPartition: Map[TopicPartition, Log] = logs.toMap

  /**
   * Map of log dir to logs by topic and partitions in that dir
   * 根据数据目录对所有日志分组
   */
  private def logsByDir = {
    this.logsByTopicPartition.groupBy {
      case (_, log) => log.dir.getParent
    }
  }

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   * 日志管理器在启动时，会启动一个定时刷写所有日志的任务
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")

    // 遍历所有log
    for ((topicPartition, log) <- logs) {
      try {
        // 虽然是定时的，但是每个日志的最近刷新时间不同，下一次刷新的时间也不同
        val timeSinceLastFlush = time.milliseconds - log.lastFlushTime
        debug("Checking if flush is needed on " + topicPartition.topic + " flush interval  " + log.config.flushMs +
              " last flushed " + log.lastFlushTime + " time since last flush: " + timeSinceLastFlush)
        // 按照一定频率  频率的阈值是long的最大值
        // 也就是kafka这不会主动把内存里面的数据写到磁盘
        // 把内存里面的数据刷到磁盘这个操作是由  操作系统完成的
        // 当然也可以去配置这个参数
        if(timeSinceLastFlush >= log.config.flushMs)
          log.flush
      } catch {
        case e: Throwable =>
          error("Error flushing topic " + topicPartition.topic, e)
      }
    }
  }
}
