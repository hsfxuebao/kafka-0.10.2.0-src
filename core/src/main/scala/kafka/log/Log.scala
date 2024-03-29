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

import kafka.api.KAFKA_0_10_0_IV0
import kafka.utils._
import kafka.common._
import kafka.metrics.KafkaMetricsGroup
import kafka.server.{BrokerTopicStats, FetchDataInfo, LogOffsetMetadata}
import java.io.{File, IOException}
import java.util.concurrent.{ConcurrentNavigableMap, ConcurrentSkipListMap}
import java.util.concurrent.atomic._
import java.text.NumberFormat

import org.apache.kafka.common.errors.{CorruptRecordException, OffsetOutOfRangeException, RecordBatchTooLargeException, RecordTooLargeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.ListOffsetRequest

import scala.collection.Seq
import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.utils.{Time, Utils}
import kafka.message.{BrokerCompressionCodec, CompressionCodec, NoCompressionCodec}
import org.apache.kafka.common.TopicPartition

object LogAppendInfo {
  val UnknownLogAppendInfo = LogAppendInfo(-1, -1, Record.NO_TIMESTAMP, -1L, Record.NO_TIMESTAMP,
    NoCompressionCodec, NoCompressionCodec, -1, -1, offsetsMonotonic = false)
}

/**
 * Struct to hold various quantities we compute about each message set before appending to the log
 *
 * @param firstOffset The first offset in the message set
 * @param lastOffset The last offset in the message set
 * @param maxTimestamp The maximum timestamp of the message set.
 * @param offsetOfMaxTimestamp The offset of the message with the maximum timestamp.
 * @param logAppendTime The log append time (if used) of the message set, otherwise Message.NoTimestamp
 * @param sourceCodec The source codec used in the message set (send by the producer)
 * @param targetCodec The target codec of the message set(after applying the broker compression configuration if any)
 * @param shallowCount The number of shallow messages
 * @param validBytes The number of valid bytes
 * @param offsetsMonotonic Are the offsets in this message set monotonically increasing
 */
case class LogAppendInfo(var firstOffset: Long,
                         var lastOffset: Long,
                         var maxTimestamp: Long,
                         var offsetOfMaxTimestamp: Long,
                         var logAppendTime: Long,
                         sourceCodec: CompressionCodec,
                         targetCodec: CompressionCodec,
                         shallowCount: Int,
                         validBytes: Int,
                         offsetsMonotonic: Boolean)


/**
 * An append-only log for storing messages.
 *
 * The log is a sequence of LogSegments, each with a base offset denoting the first message in the segment.
 *
 * New log segments are created according to a configurable policy that controls the size in bytes or time interval
 * for a given segment.
 *
 * @param dir The directory in which log segments are created.
 * @param config The log configuration settings
 * @param recoveryPoint The offset at which to begin recovery--i.e. the first offset which has not been flushed to disk
 * @param scheduler The thread pool scheduler used for background actions
 * @param time The time instance used for checking the clock
 * 日志对应的目录
 */
@threadsafe
class Log(@volatile var dir: File,
          @volatile var config: LogConfig,
          @volatile var recoveryPoint: Long = 0L,
          scheduler: Scheduler,
          time: Time = Time.SYSTEM) extends Logging with KafkaMetricsGroup {

  import kafka.log.Log._

  /* A lock that guards all modifications to the log */
  private val lock = new Object

  /* last time it was flushed */
  private val lastflushedTime = new AtomicLong(time.milliseconds)

  def initFileSize() : Int = {
    if (config.preallocate)
      config.segmentSize
    else
      0
  }

  // 追加消息集到日志
  @volatile private var nextOffsetMetadata: LogOffsetMetadata = _

  /* the actual segments of the log */
  /**
   * juc 下数据结构 跳表实现的一个并发安全的Map集合
   * key: 文件名（base offset）
   * value : 就是一个segment
   * 目的就是为了可以根据offset的大小快速定位
   * 日志包含多个日志分段,字典数据结构的键是日志分段的基准偏移量
   */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
  locally {
    val startMs = time.milliseconds

    loadSegments()
    /* Calculate the offset of the next message */
    // activeSegment.nextOffset() 下一条消息的偏移量 activeSegment.baseOffset 日志分段的基准偏移量
    // activeSegment.size.toInt 日志分段的大小
    nextOffsetMetadata = new LogOffsetMetadata(activeSegment.nextOffset(), activeSegment.baseOffset,
      activeSegment.size.toInt)

    info("Completed load of log %s with %d log segments and log end offset %d in %d ms"
      .format(name, segments.size(), logEndOffset, time.milliseconds - startMs))
  }

  // 一个日志的目录对应一个分区
  val topicPartition: TopicPartition = Log.parseTopicPartitionName(dir)

  private val tags = Map("topic" -> topicPartition.topic, "partition" -> topicPartition.partition.toString)

  newGauge("NumLogSegments",
    new Gauge[Int] {
      def value = numberOfSegments
    },
    tags)

  newGauge("LogStartOffset",
    new Gauge[Long] {
      def value = logStartOffset
    },
    tags)

  newGauge("LogEndOffset",
    new Gauge[Long] {
      def value = logEndOffset
    },
    tags)

  newGauge("Size",
    new Gauge[Long] {
      def value = size
    },
    tags)

  /** The name of this log */
  def name  = dir.getName()

  /* Load the log segments from the log files on disk */
  private def loadSegments() {
    // create the log directory if it doesn't exist
    dir.mkdirs()
    var swapFiles = Set[File]()

    // first do a pass through the files in the log directory and remove any temporary files
    // and find any interrupted swap operations
    for(file <- dir.listFiles if file.isFile) {
      if(!file.canRead)
        throw new IOException("Could not read file " + file)
      val filename = file.getName
      if(filename.endsWith(DeletedFileSuffix) || filename.endsWith(CleanedFileSuffix)) {
        // if the file ends in .deleted or .cleaned, delete it
        file.delete()
      } else if(filename.endsWith(SwapFileSuffix)) {
        // we crashed in the middle of a swap operation, to recover:
        // if a log, delete the .index file, complete the swap operation later
        // if an index just delete it, it will be rebuilt
        val baseName = new File(CoreUtils.replaceSuffix(file.getPath, SwapFileSuffix, ""))
        if(baseName.getPath.endsWith(IndexFileSuffix)) {
          file.delete()
        } else if(baseName.getPath.endsWith(LogFileSuffix)){
          // delete the index
          val index = new File(CoreUtils.replaceSuffix(baseName.getPath, LogFileSuffix, IndexFileSuffix))
          index.delete()
          swapFiles += file
        }
      }
    }

    // now do a second pass and load all the .log and all index files
    for(file <- dir.listFiles if file.isFile) {
      val filename = file.getName
      if(filename.endsWith(IndexFileSuffix) || filename.endsWith(TimeIndexFileSuffix)) {
        // if it is an index file, make sure it has a corresponding .log file
        val logFile =
          if (filename.endsWith(TimeIndexFileSuffix))
            new File(file.getAbsolutePath.replace(TimeIndexFileSuffix, LogFileSuffix))
          else
            new File(file.getAbsolutePath.replace(IndexFileSuffix, LogFileSuffix))

        if(!logFile.exists) {
          warn("Found an orphaned index file, %s, with no corresponding log file.".format(file.getAbsolutePath))
          file.delete()
        }
      } else if(filename.endsWith(LogFileSuffix)) {
        // if its a log file, load the corresponding log segment
        val start = filename.substring(0, filename.length - LogFileSuffix.length).toLong
        val indexFile = Log.indexFilename(dir, start)
        val timeIndexFile = Log.timeIndexFilename(dir, start)

        val indexFileExists = indexFile.exists()
        val timeIndexFileExists = timeIndexFile.exists()
        val segment = new LogSegment(dir = dir,
                                     startOffset = start,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = true)

        if (indexFileExists) {
          try {
            segment.index.sanityCheck()
            // Resize the time index file to 0 if it is newly created.
            if (!timeIndexFileExists)
              segment.timeIndex.resize(0)
            segment.timeIndex.sanityCheck()
          } catch {
            case e: java.lang.IllegalArgumentException =>
              warn(s"Found a corrupted index file due to ${e.getMessage}}. deleting ${timeIndexFile.getAbsolutePath}, " +
                s"${indexFile.getAbsolutePath} and rebuilding index...")
              indexFile.delete()
              timeIndexFile.delete()
              segment.recover(config.maxMessageSize)
          }
        } else {
          error("Could not find index file corresponding to log file %s, rebuilding index...".format(segment.log.file.getAbsolutePath))
          segment.recover(config.maxMessageSize)
        }
        segments.put(start, segment)
      }
    }

    // Finally, complete any interrupted swap operations. To be crash-safe,
    // log files that are replaced by the swap segment should be renamed to .deleted
    // before the swap file is restored as the new segment file.
    for (swapFile <- swapFiles) {
      val logFile = new File(CoreUtils.replaceSuffix(swapFile.getPath, SwapFileSuffix, ""))
      val fileName = logFile.getName
      val startOffset = fileName.substring(0, fileName.length - LogFileSuffix.length).toLong
      val indexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, IndexFileSuffix) + SwapFileSuffix)
      val index =  new OffsetIndex(indexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val timeIndexFile = new File(CoreUtils.replaceSuffix(logFile.getPath, LogFileSuffix, TimeIndexFileSuffix) + SwapFileSuffix)
      val timeIndex = new TimeIndex(timeIndexFile, baseOffset = startOffset, maxIndexSize = config.maxIndexSize)
      val swapSegment = new LogSegment(FileRecords.open(swapFile),
                                       index = index,
                                       timeIndex = timeIndex,
                                       baseOffset = startOffset,
                                       indexIntervalBytes = config.indexInterval,
                                       rollJitterMs = config.randomSegmentJitter,
                                       time = time)
      info("Found log file %s from interrupted swap operation, repairing.".format(swapFile.getPath))
      swapSegment.recover(config.maxMessageSize)
      val oldSegments = logSegments(swapSegment.baseOffset, swapSegment.nextOffset)
      replaceSegments(swapSegment, oldSegments.toSeq, isRecoveredSwapFile = true)
    }

    if(logSegments.isEmpty) {
      // no existing segments, create a new mutable segment beginning at offset 0
      segments.put(0L, new LogSegment(dir = dir,
                                     startOffset = 0,
                                     indexIntervalBytes = config.indexInterval,
                                     maxIndexSize = config.maxIndexSize,
                                     rollJitterMs = config.randomSegmentJitter,
                                     time = time,
                                     fileAlreadyExists = false,
                                     initFileSize = this.initFileSize(),
                                     preallocate = config.preallocate))
    } else {
      if (!dir.getAbsolutePath.endsWith(Log.DeleteDirSuffix)) {
        recoverLog()
        // reset the index size of the currently active log segment to allow more entries
        activeSegment.index.resize(config.maxIndexSize)
        activeSegment.timeIndex.resize(config.maxIndexSize)
      }
    }
  }

  /**
   * 更新日志"最近偏移量"，传入的参数一般是最后一条消息的偏移量+1
   * 使用方需要获取日志"最近偏移量"时，就不需要再做+1的操作
   * @param messageOffset
   */
  private def updateLogEndOffset(messageOffset: Long) {
    nextOffsetMetadata = new LogOffsetMetadata(messageOffset, activeSegment.baseOffset, activeSegment.size.toInt)
  }

  private def recoverLog() {
    // if we have the clean shutdown marker, skip recovery
    if(hasCleanShutdownFile) {
      this.recoveryPoint = activeSegment.nextOffset
      return
    }

    // okay we need to actually recovery this log
    val unflushed = logSegments(this.recoveryPoint, Long.MaxValue).iterator
    while(unflushed.hasNext) {
      val curr = unflushed.next
      info("Recovering unflushed segment %d in log %s.".format(curr.baseOffset, name))
      val truncatedBytes =
        try {
          curr.recover(config.maxMessageSize)
        } catch {
          case _: InvalidOffsetException =>
            val startOffset = curr.baseOffset
            warn("Found invalid offset during recovery for log " + dir.getName +". Deleting the corrupt segment and " +
                 "creating an empty one with starting offset " + startOffset)
            curr.truncateTo(startOffset)
        }
      if(truncatedBytes > 0) {
        // we had an invalid message, delete all remaining log
        warn("Corruption found in segment %d of log %s, truncating to offset %d.".format(curr.baseOffset, name, curr.nextOffset))
        unflushed.foreach(deleteSegment)
      }
    }
  }

  /**
   * Check if we have the "clean shutdown" file
   */
  private def hasCleanShutdownFile = new File(dir.getParentFile, CleanShutdownFile).exists()

  /**
   * The number of segments in the log.
   * Take care! this is an O(n) operation.
   */
  def numberOfSegments: Int = segments.size

  /**
   * Close this log
   */
  def close() {
    debug("Closing log " + name)
    lock synchronized {
      logSegments.foreach(_.close())
    }
  }

  /**
   * Append this message set to the active segment of the log, rolling over to a fresh segment if necessary.
   *
   * This method will generally be responsible for assigning offsets to the messages,
   * however if the assignOffsets=false flag is passed we will only check that the existing offsets are valid.
   *
   * @param records The log records to append
   * @param assignOffsets Should the log assign offsets to this message set or blindly apply what it is given
   * @throws KafkaStorageException If the append fails due to an I/O error.
   * @return Information about the appended messages including the first and last offset.
   */
  def append(records: MemoryRecords, assignOffsets: Boolean = true): LogAppendInfo = {
    // todo 步骤一 检验数据（Producer -> kafka）
    val appendInfo = analyzeAndValidateRecords(records)

    // if we have any valid messages, append them to the log
    if (appendInfo.shallowCount == 0)
      return appendInfo

    // trim any invalid bytes or partial messages before appending it to the on-disk log
    var validRecords = trimInvalidBytes(records, appendInfo)

    try {
      // they are valid, insert them in the log
      lock synchronized {

        if (assignOffsets) {
          // assign offsets to the message set
          // todo 步骤二  分配offset
          val offset = new LongRef(nextOffsetMetadata.messageOffset)
          // 获取最新的"下一个偏移量"作为第一条消息的绝对偏移量
          appendInfo.firstOffset = offset.value
          val now = time.milliseconds
          val validateAndOffsetAssignResult = try {
            LogValidator.validateMessagesAndAssignOffsets(validRecords,
                                                          offset,
                                                          now,
                                                          appendInfo.sourceCodec,
                                                          appendInfo.targetCodec,
                                                          config.compact,
                                                          config.messageFormatVersion.messageFormatVersion,
                                                          config.messageTimestampType,
                                                          config.messageTimestampDifferenceMaxMs)
          } catch {
            case e: IOException => throw new KafkaException("Error in validating messages while appending to log '%s'".format(name), e)
          }
          // todo 步骤三 获取合法的数据
          validRecords = validateAndOffsetAssignResult.validatedRecords
          appendInfo.maxTimestamp = validateAndOffsetAssignResult.maxTimestamp
          appendInfo.offsetOfMaxTimestamp = validateAndOffsetAssignResult.shallowOffsetOfMaxTimestamp
          appendInfo.lastOffset = offset.value - 1
          if (config.messageTimestampType == TimestampType.LOG_APPEND_TIME)
            appendInfo.logAppendTime = now

          // re-validate message sizes if there's a possibility that they have changed (due to re-compression or message
          // format conversion)
          if (validateAndOffsetAssignResult.messageSizeMaybeChanged) {
            for (logEntry <- validRecords.shallowEntries.asScala) {
              if (logEntry.sizeInBytes > config.maxMessageSize) {
                // we record the original message set size instead of the trimmed size
                // to be consistent with pre-compression bytesRejectedRate recording
                BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
                BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
                throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
                  .format(logEntry.sizeInBytes, config.maxMessageSize))
              }
            }
          }

        } else {
          // we are taking the offsets we are given
          if (!appendInfo.offsetsMonotonic || appendInfo.firstOffset < nextOffsetMetadata.messageOffset)
            throw new IllegalArgumentException("Out of order offsets found in " + records.deepEntries.asScala.map(_.offset))
        }

        // check messages set size may be exceed config.segmentSize
        if (validRecords.sizeInBytes > config.segmentSize) {
          throw new RecordBatchTooLargeException("Message set size is %d bytes which exceeds the maximum configured segment size of %d."
            .format(validRecords.sizeInBytes, config.segmentSize))
        }

        // maybe roll the log if this segment is full
        // todo 步骤四 获取可用的segment
        val segment = maybeRoll(messagesSize = validRecords.sizeInBytes,
          maxTimestampInMessages = appendInfo.maxTimestamp,
          maxOffsetInMessages = appendInfo.lastOffset)


        // now append to the log
        // todo 步骤五 把数据写到segment
        segment.append(firstOffset = appendInfo.firstOffset,
          largestOffset = appendInfo.lastOffset,
          largestTimestamp = appendInfo.maxTimestamp,
          shallowOffsetOfMaxTimestamp = appendInfo.offsetOfMaxTimestamp,
          records = validRecords)

        // increment the log end offset
        // todo 步骤六 更新LEO
        // LEO = lastOffset + 1
        updateLogEndOffset(appendInfo.lastOffset + 1)

        trace("Appended message set to log %s with first offset: %d, next offset: %d, and messages: %s"
          .format(this.name, appendInfo.firstOffset, nextOffsetMetadata.messageOffset, validRecords))
        // todo 步骤七  根据条件判断 把内存里面的数据写到磁盘
        // 假设我们配置的10分钟刷写磁盘
        // LogManager -> startup -> 操作系统里面的机制
        // unflushedMessages 未刷新消息数量的计算方式是：最新偏移量-上次的检查点位置
        if (unflushedMessages >= config.flushInterval)
          /**
           * flushInterval 默认是long型最大值，所以基本不会执行这个操作
           * 从内存里面刷写数据到磁盘的操作就交给操作系统，由操作系统去管理
           * 操作系统本身有一些机制，也会定期把数据写到磁盘中
           */
          flush()

        appendInfo
      }
    } catch {
      case e: IOException => throw new KafkaStorageException("I/O exception in append to log '%s'".format(name), e)
    }
  }

  /**
   * Validate the following:
   * <ol>
   * <li> each message matches its CRC
   * <li> each message size is valid
   * </ol>
   *
   * Also compute the following quantities:
   * <ol>
   * <li> First offset in the message set
   * <li> Last offset in the message set
   * <li> Number of messages
   * <li> Number of valid bytes
   * <li> Whether the offsets are monotonically increasing
   * <li> Whether any compression codec is used (if many are used, then the last one is given)
   * </ol>
   */
  private def analyzeAndValidateRecords(records: MemoryRecords): LogAppendInfo = {
    var shallowMessageCount = 0 // 消息数量
    var validBytesCount = 0 // 有效字节数
    // 第一条消息和最后一条（循环时表示上一条消息的偏移量）消息的偏移量
    var firstOffset = -1L
    var lastOffset = -1L
    var sourceCodec: CompressionCodec = NoCompressionCodec
    var monotonic = true  // 是否单调递增
    var maxTimestamp = Record.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    for (entry <- records.shallowEntries.asScala) {
      // update the first offset if on the first message
      // 在第一条消息中更新firstOffset
      if(firstOffset < 0)
        firstOffset = entry.offset
      // check that offsets are monotonically increasing
      if(lastOffset >= entry.offset)
        monotonic = false
      // update the last offset seen
      // 每循环一条消息，就更新lastOffset
      lastOffset = entry.offset

      val record = entry.record

      // Check if the message sizes are valid.
      val messageSize = entry.sizeInBytes
      if(messageSize > config.maxMessageSize) {
        BrokerTopicStats.getBrokerTopicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesRejectedRate.mark(records.sizeInBytes)
        throw new RecordTooLargeException("Message size is %d bytes which exceeds the maximum configured message size of %d."
          .format(messageSize, config.maxMessageSize))
      }

      // check the validity of the message by checking CRC
      // 检查消息是否有效
      record.ensureValid()
      if (record.timestamp > maxTimestamp) {
        maxTimestamp = record.timestamp
        offsetOfMaxTimestamp = lastOffset
      }
      shallowMessageCount += 1
      validBytesCount += messageSize

      val messageCodec = CompressionCodec.getCompressionCodec(record.compressionType.id)
      if (messageCodec != NoCompressionCodec)
        sourceCodec = messageCodec
    }

    // Apply broker-side compression if any
    val targetCodec = BrokerCompressionCodec.getTargetCompressionCodec(config.compressionType, sourceCodec)

    LogAppendInfo(firstOffset, lastOffset, maxTimestamp, offsetOfMaxTimestamp, Record.NO_TIMESTAMP, sourceCodec,
      targetCodec, shallowMessageCount, validBytesCount, monotonic)
  }

  /**
   * Trim any invalid bytes from the end of this message set (if there are any)
   *
   * @param records The records to trim
   * @param info The general information of the message set
   * @return A trimmed message set. This may be the same as what was passed in or it may not.
   */
  private def trimInvalidBytes(records: MemoryRecords, info: LogAppendInfo): MemoryRecords = {
    val validBytes = info.validBytes
    if (validBytes < 0)
      throw new CorruptRecordException("Illegal length of message set " + validBytes + " Message set cannot be appended to log. Possible causes are corrupted produce requests")
    if (validBytes == records.sizeInBytes) {
      records
    } else {
      // trim invalid bytes
      val validByteBuffer = records.buffer.duplicate()
      validByteBuffer.limit(validBytes)
      MemoryRecords.readableRecords(validByteBuffer)
    }
  }

  /**
   * Read messages from the log.
   *
   * @param startOffset The offset to begin reading at 从指定的起始偏移量读取日志
   * @param maxLength The maximum number of bytes to read fetchSize 默认值是1M
   * @param maxOffset The offset to read up to, exclusive. (i.e. this offset NOT included in the resulting message set) 最大偏移量
   * @param minOneMessage If this is true, the first message will be returned even if it exceeds `maxLength` (if one exists)
   *
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read.
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None, minOneMessage: Boolean = false): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // Because we don't use lock for reading, the synchronization is a little bit tricky.
    // We create the local variables to avoid race conditions with updates to the log.
    val currentNextOffsetMetadata = nextOffsetMetadata
    val next = currentNextOffsetMetadata.messageOffset
    if(startOffset == next)
      return FetchDataInfo(currentNextOffsetMetadata, MemoryRecords.EMPTY)
    // 获取对应的segment对象，先找日志分段
    var entry = segments.floorEntry(startOffset)

    // attempt to read beyond the log end offset is an error
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

    // Do the read on the segment with a base offset less than the target offset
    // but if that segment doesn't contain any messages with an offset greater than that
    // continue to read from successive segments until we get some messages or we reach the end of the log
    while(entry != null) {
      // If the fetch occurs on the active segment, there might be a race condition where two fetch requests occur after
      // the message is appended but before the nextOffsetMetadata is updated. In that case the second fetch may
      // cause OffsetOutOfRangeException. To solve that, we cap the reading up to exposed position instead of the log
      // end of the active segment.
      val maxPosition = {
        if (entry == segments.lastEntry) {
          val exposedPos = nextOffsetMetadata.relativePositionInSegment.toLong
          // Check the segment again in case a new segment has just rolled out.
          if (entry != segments.lastEntry)
            // New log segment has rolled out, we can read up to the file end.
            entry.getValue.size
          else
            exposedPos
        } else {
          entry.getValue.size
        }
      }
      // todo 核心代码 通过segment去读取上面的数据
      val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition, minOneMessage)
      if(fetchInfo == null) {
        // 如果日志分段没有读到数据，会去读取更高的分段
        entry = segments.higherEntry(entry.getKey)
      } else {
        return fetchInfo
      }
    }

    // okay we are beyond the end of the last segment with no data fetched although the start offset is in range,
    // this can happen when all messages with offset larger than start offsets have been deleted.
    // In this case, we will return the empty set with log end offset metadata
    FetchDataInfo(nextOffsetMetadata, MemoryRecords.EMPTY)
  }

  /**
   * Get an offset based on the given timestamp
   * The offset returned is the offset of the first message whose timestamp is greater than or equals to the
   * given timestamp.
   *
   * If no such message is found, the log end offset is returned.
   *
   * `NOTE:` OffsetRequest V0 does not use this method, the behavior of OffsetRequest V0 remains the same as before
   * , i.e. it only gives back the timestamp based on the last modification time of the log segments.
   *
   * @param targetTimestamp The given timestamp for offset fetching.
   * @return The offset of the first message whose timestamp is greater than or equals to the given timestamp.
   *         None if no such message is found.
   */
  def fetchOffsetsByTimestamp(targetTimestamp: Long): Option[TimestampOffset] = {
    debug(s"Searching offset for timestamp $targetTimestamp")

    if (config.messageFormatVersion < KAFKA_0_10_0_IV0 &&
        targetTimestamp != ListOffsetRequest.EARLIEST_TIMESTAMP &&
        targetTimestamp != ListOffsetRequest.LATEST_TIMESTAMP)
      throw new UnsupportedForMessageFormatException(s"Cannot search offsets based on timestamp because message format version " +
          s"for partition $topicPartition is ${config.messageFormatVersion} which is earlier than the minimum " +
          s"required version $KAFKA_0_10_0_IV0")

    // Cache to avoid race conditions. `toBuffer` is faster than most alternatives and provides
    // constant time access while being safe to use with concurrent collections unlike `toArray`.
    val segmentsCopy = logSegments.toBuffer
    // For the earliest and latest, we do not need to return the timestamp.
    if (targetTimestamp == ListOffsetRequest.EARLIEST_TIMESTAMP)
        return Some(TimestampOffset(Record.NO_TIMESTAMP, segmentsCopy.head.baseOffset))
    else if (targetTimestamp == ListOffsetRequest.LATEST_TIMESTAMP)
        return Some(TimestampOffset(Record.NO_TIMESTAMP, logEndOffset))

    val targetSeg = {
      // Get all the segments whose largest timestamp is smaller than target timestamp
      val earlierSegs = segmentsCopy.takeWhile(_.largestTimestamp < targetTimestamp)
      // We need to search the first segment whose largest timestamp is greater than the target timestamp if there is one.
      if (earlierSegs.length < segmentsCopy.length)
        Some(segmentsCopy(earlierSegs.length))
      else
        None
    }

    targetSeg.flatMap(_.findOffsetByTimestamp(targetTimestamp))
  }

  /**
   * Given a message offset, find its corresponding offset metadata in the log.
   * If the message offset is out of range, return unknown offset metadata
   */
  def convertToOffsetMetadata(offset: Long): LogOffsetMetadata = {
    try {
      val fetchDataInfo = read(offset, 1)
      fetchDataInfo.fetchOffsetMetadata
    } catch {
      case _: OffsetOutOfRangeException => LogOffsetMetadata.UnknownOffsetMetadata
    }
  }

  /**
   * Delete any log segments matching the given predicate function,
   * starting with the oldest segment and moving forward until a segment doesn't match.
   *
   * @param predicate A function that takes in a single log segment and returns true iff it is deletable
   * @return The number of segments deleted
   */
  private def deleteOldSegments(predicate: LogSegment => Boolean): Int = {
    lock synchronized {
      val deletable = deletableSegments(predicate)
      val numToDelete = deletable.size
      if (numToDelete > 0) {
        // we must always have at least one segment, so if we are going to delete all the segments, create a new one first
        if (segments.size == numToDelete)
          roll()
        // remove the segments for lookups
        // deleteSegment 方法
        deletable.foreach(deleteSegment)
      }
      numToDelete
    }
  }

  /**
    * Find segments starting from the oldest until the the user-supplied predicate is false.
    * A final segment that is empty will never be returned (since we would just end up re-creating it).
    * @param predicate A function that takes in a single log segment and returns true iff it is deletable
    * @return the segments ready to be deleted
    */
  private def deletableSegments(predicate: LogSegment => Boolean) = {
    val lastEntry = segments.lastEntry
    if (lastEntry == null) Seq.empty
    else logSegments.takeWhile(s => predicate(s) && (s.baseOffset != lastEntry.getValue.baseOffset || s.size > 0))
  }

  /**
    * Delete any log segments that have either expired due to time based retention
    * or because the log size is > retentionSize
    */
  def deleteOldSegments(): Int = {
    if (!config.delete) return 0
    //
    deleteRetenionMsBreachedSegments() + deleteRetentionSizeBreachedSegments()
  }

  private def deleteRetenionMsBreachedSegments() : Int = {
    if (config.retentionMs < 0) return 0
    val startMs = time.milliseconds
    // 根据时间周期来删除文件  判断时间周期
    // 如果超出周期，就把对应的文件删除 默认7天
    deleteOldSegments(startMs - _.largestTimestamp > config.retentionMs)
  }

  private def deleteRetentionSizeBreachedSegments() : Int = {
    if (config.retentionSize < 0 || size < config.retentionSize) return 0
    var diff = size - config.retentionSize
    def shouldDelete(segment: LogSegment) = {
      if (diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    // 根据文件大小去删除文件
    deleteOldSegments(shouldDelete)
  }

  /**
   * The size of the log in bytes
   */
  def size: Long = logSegments.map(_.size).sum

   /**
   * The earliest message offset in the log
   */
  def logStartOffset: Long = logSegments.head.baseOffset

  /**
   * The offset metadata of the next message that will be appended to the log
   */
  def logEndOffsetMetadata: LogOffsetMetadata = nextOffsetMetadata

  /**
   *  The offset of the next message that will be appended to the log
   *  下一条消息的偏移量，取自"下一条偏移量元数据"中的第一个字段值
   */
  def logEndOffset: Long = nextOffsetMetadata.messageOffset

  /**
   * Roll the log over to a new empty log segment if necessary.
   *
   * @param messagesSize The messages set size in bytes
   * @param maxTimestampInMessages The maximum timestamp in the messages.
   * logSegment will be rolled if one of the following conditions met
   * <ol>
   * <li> The logSegment is full
   * <li> The maxTime has elapsed since the timestamp of first message in the segment (or since the create time if
   * the first message does not have a timestamp)
   * <li> The index is full
   * </ol>
   * @return The currently active segment after (perhaps) rolling to a new segment
   */
  private def maybeRoll(messagesSize: Int, maxTimestampInMessages: Long, maxOffsetInMessages: Long): LogSegment = {
    // 获取当前最新的segment
    val segment = activeSegment
    val now = time.milliseconds
    val reachedRollMs = segment.timeWaitedForRoll(now, maxTimestampInMessages) > config.segmentMs - segment.rollJitterMs
    if (
        // 第一个条件 kafka默认一个segment的大小是1G
        // 如果当前segment的大小在加上 将要写入进去的数据大小 超过1G 就会新建一个segment
        segment.size > config.segmentSize - messagesSize ||
          // 第二个条件 每隔一段时间都会新建一个
          // 这个策略我们一般是不用的
        (segment.size > 0 && reachedRollMs) ||
          // 第三个条件 index 或 timeIndex满了
        segment.index.isFull || segment.timeIndex.isFull || !segment.canConvertToRelativeOffset(maxOffsetInMessages)) {
      debug(s"Rolling new log segment in $name (log_size = ${segment.size}/${config.segmentSize}}, " +
          s"index_size = ${segment.index.entries}/${segment.index.maxEntries}, " +
          s"time_index_size = ${segment.timeIndex.entries}/${segment.timeIndex.maxEntries}, " +
          s"inactive_time_ms = ${segment.timeWaitedForRoll(now, maxTimestampInMessages)}/${config.segmentMs - segment.rollJitterMs}).")
      /*
        maxOffsetInMessages - Integer.MAX_VALUE is a heuristic value for the first offset in the set of messages.
        Since the offset in messages will not differ by more than Integer.MAX_VALUE, this is guaranteed <= the real
        first offset in the set. Determining the true first offset in the set requires decompression, which the follower
        is trying to avoid during log append. Prior behavior assigned new baseOffset = logEndOffset from old segment.
        This was problematic in the case that two consecutive messages differed in offset by
        Integer.MAX_VALUE.toLong + 2 or more.  In this case, the prior behavior would roll a new log segment whose
        base offset was too low to contain the next message.  This edge case is possible when a replica is recovering a
        highly compacted topic from scratch.
       */
      // 新建一个segment
      roll(maxOffsetInMessages - Integer.MAX_VALUE)
    } else {
      // 直接返回刚刚获取的最新的segment 说明这个segment是可用的
      segment
    }
  }

  /**
   * Roll the log over to a new active segment starting with the current logEndOffset.
   * This will trim the index to the exact size of the number of entries it currently contains.
   *
   * @return The newly rolled segment
   * 创建新的日志分段，并将其添加到日志管理额segment字典中
   */
  def roll(expectedNextOffset: Long = 0): LogSegment = {
    val start = time.nanoseconds
    lock synchronized {
      /**
       * LEO = lastoffset + 1
       * 获取LEO的值作为最新的一个偏移量
       * 举例子 lasfoffset=10001
       * LEO = 10002 我们这新获取一个偏移量的时候 用的10002这个值
       */
      val newOffset = Math.max(expectedNextOffset, logEndOffset)
      // 新建一个文件 用LEO的名字作为文件名
      val logFile = logFilename(dir, newOffset)
      // 创建索引文件
      val indexFile = indexFilename(dir, newOffset)
      val timeIndexFile = timeIndexFilename(dir, newOffset)
      // 如果文件已经存在  那么删除文件
      for(file <- List(logFile, indexFile, timeIndexFile); if file.exists) {
        warn("Newly rolled segment file " + file.getName + " already exists; deleting it first")
        file.delete()
      }

      segments.lastEntry() match {
        case null =>
        case entry => {
          val seg = entry.getValue
          seg.onBecomeInactiveSegment()
          seg.index.trimToValidSize()
          seg.timeIndex.trimToValidSize()
          seg.log.trim()
        }
      }
      // 新建出来的LogSegment
      val segment = new LogSegment(dir,
                                   startOffset = newOffset,
                                   indexIntervalBytes = config.indexInterval,
                                   maxIndexSize = config.maxIndexSize,
                                   rollJitterMs = config.randomSegmentJitter,
                                   time = time,
                                   fileAlreadyExists = false,
                                   initFileSize = initFileSize,
                                   preallocate = config.preallocate)
      // todo 把segment添加到某一个数据结构里面
      val prev = addSegment(segment)
      if(prev != null)
        throw new KafkaException("Trying to roll a new log segment for topic partition %s with start offset %d while it already exists.".format(name, newOffset))
      // We need to update the segment base offset and append position data of the metadata when log rolls.
      // The next offset should not change.
      updateLogEndOffset(nextOffsetMetadata.messageOffset)
      // schedule an asynchronous flush of the old segment
      // 立即启动定时刷写任务
      scheduler.schedule("flush-log", () => flush(newOffset), delay = 0L)

      info("Rolled new log segment for '" + name + "' in %.0f ms.".format((System.nanoTime - start) / (1000.0*1000.0)))
      // 返回值是新创建的日志分段
      segment
    }
  }

  /**
   * The number of messages appended to the log since the last flush
   */
  def unflushedMessages() = this.logEndOffset - this.recoveryPoint

  /**
   * Flush all log segments
   * 获取最近的偏移量，刷新上一次检查点到最近偏移量之间的所有日志分段
   */
  def flush(): Unit = flush(this.logEndOffset)

  /**
   * Flush log segments for all offsets up to offset-1
   *
   * @param offset The offset to flush up to (non-inclusive); the new recovery point
   */
  def flush(offset: Long) : Unit = {
    if (offset <= this.recoveryPoint)
      return
    debug("Flushing log '" + name + " up to offset " + offset + ", last flushed: " + lastFlushTime + " current time: " +
          time.milliseconds + " unflushed = " + unflushedMessages)
    // 遍历所有当前主机的所有的segment
    for(segment <- logSegments(this.recoveryPoint, offset))
      // 调用flush方法，刷新数据文件和索引文件（调用操作系统的fsync）
      segment.flush()
    lock synchronized {
      if(offset > this.recoveryPoint) {
        this.recoveryPoint = offset
        // 更新最近的刷新时间
        lastflushedTime.set(time.milliseconds)
      }
    }
  }

  /**
   * Completely delete this log directory and all contents from the file system with no delay
   */
  private[log] def delete() {
    lock synchronized {
      logSegments.foreach(_.delete())
      segments.clear()
      Utils.delete(dir)
    }
  }

  /**
   * Truncate this log so that it ends with the greatest offset < targetOffset.
   *
   * @param targetOffset The offset to truncate to, an upper bound on all offsets in the log after truncation is complete.
   */
  private[log] def truncateTo(targetOffset: Long) {
    info("Truncating log %s to offset %d.".format(name, targetOffset))
    if(targetOffset < 0)
      throw new IllegalArgumentException("Cannot truncate to a negative offset (%d).".format(targetOffset))
    if(targetOffset > logEndOffset) {
      info("Truncating %s to %d has no effect as the largest offset in the log is %d.".format(name, targetOffset, logEndOffset-1))
      return
    }
    lock synchronized {
      if(segments.firstEntry.getValue.baseOffset > targetOffset) {
        truncateFullyAndStartAt(targetOffset)
      } else {
        val deletable = logSegments.filter(segment => segment.baseOffset > targetOffset)
        deletable.foreach(deleteSegment)
        activeSegment.truncateTo(targetOffset)
        updateLogEndOffset(targetOffset)
        this.recoveryPoint = math.min(targetOffset, this.recoveryPoint)
      }
    }
  }

  /**
   *  Delete all data in the log and start at the new offset
   *
   *  @param newOffset The new offset to start the log with
   */
  private[log] def truncateFullyAndStartAt(newOffset: Long) {
    debug("Truncate and start log '" + name + "' to " + newOffset)
    lock synchronized {
      val segmentsToDelete = logSegments.toList
      segmentsToDelete.foreach(deleteSegment)
      addSegment(new LogSegment(dir,
                                newOffset,
                                indexIntervalBytes = config.indexInterval,
                                maxIndexSize = config.maxIndexSize,
                                rollJitterMs = config.randomSegmentJitter,
                                time = time,
                                fileAlreadyExists = false,
                                initFileSize = initFileSize,
                                preallocate = config.preallocate))
      updateLogEndOffset(newOffset)
      this.recoveryPoint = math.min(newOffset, this.recoveryPoint)
    }
  }

  /**
   * The time this log is last known to have been fully flushed to disk
   */
  def lastFlushTime(): Long = lastflushedTime.get

  /**
   * The active segment that is currently taking appends
   * 任何时刻，只会有一个活动的日志分段
   */
  def activeSegment = segments.lastEntry.getValue

  /**
   * All the log segments in this log ordered from oldest to newest
   */
  def logSegments: Iterable[LogSegment] = segments.values.asScala

  /**
   * Get all segments beginning with the segment that includes "from" and ending with the segment
   * that includes up to "to-1" or the end of the log (if to > logEndOffset)
   */
  def logSegments(from: Long, to: Long): Iterable[LogSegment] = {
    lock synchronized {
      val floor = segments.floorKey(from)
      if(floor eq null)
        segments.headMap(to).values.asScala
      else
        segments.subMap(floor, true, to, false).values.asScala
    }
  }

  override def toString = "Log(" + dir + ")"

  /**
   * This method performs an asynchronous log segment delete by doing the following:
   * <ol>
   *   <li>It removes the segment from the segment map so that it will no longer be used for reads.
   *   <li>It renames the index and log files by appending .deleted to the respective file name
   *   <li>It schedules an asynchronous delete operation to occur in the future
   * </ol>
   * This allows reads to happen concurrently without synchronization and without the possibility of physically
   * deleting a file while it is being read from.
   *
   * @param segment The log segment to schedule for deletion
   */
  private def deleteSegment(segment: LogSegment) {
    info("Scheduling log segment %d for log %s for deletion.".format(segment.baseOffset, name))
    lock synchronized {
      // 从映射关系表中删除数据
      segments.remove(segment.baseOffset)
      // 异步删除日志分段
      asyncDeleteSegment(segment)
    }
  }

  /**
   * Perform an asynchronous delete on the given file if it exists (otherwise do nothing)
   *
   * @throws KafkaStorageException if the file can't be renamed and still exists
   */
  private def asyncDeleteSegment(segment: LogSegment) {
    segment.changeFileSuffixes("", Log.DeletedFileSuffix)
    def deleteSeg() {
      info("Deleting segment %d from log %s.".format(segment.baseOffset, name))
      segment.delete()
    }
    scheduler.schedule("delete-file", deleteSeg, delay = config.fileDeleteDelayMs)
  }

  /**
   * Swap a new segment in place and delete one or more existing segments in a crash-safe manner. The old segments will
   * be asynchronously deleted.
   *
   * The sequence of operations is:
   * <ol>
   *   <li> Cleaner creates new segment with suffix .cleaned and invokes replaceSegments().
   *        If broker crashes at this point, the clean-and-swap operation is aborted and
   *        the .cleaned file is deleted on recovery in loadSegments().
   *   <li> New segment is renamed .swap. If the broker crashes after this point before the whole
   *        operation is completed, the swap operation is resumed on recovery as described in the next step.
   *   <li> Old segment files are renamed to .deleted and asynchronous delete is scheduled.
   *        If the broker crashes, any .deleted files left behind are deleted on recovery in loadSegments().
   *        replaceSegments() is then invoked to complete the swap with newSegment recreated from
   *        the .swap file and oldSegments containing segments which were not renamed before the crash.
   *   <li> Swap segment is renamed to replace the existing segment, completing this operation.
   *        If the broker crashes, any .deleted files which may be left behind are deleted
   *        on recovery in loadSegments().
   * </ol>
   *
   * @param newSegment The new log segment to add to the log
   * @param oldSegments The old log segments to delete from the log
   * @param isRecoveredSwapFile true if the new segment was created from a swap file during recovery after a crash
   */
  private[log] def replaceSegments(newSegment: LogSegment, oldSegments: Seq[LogSegment], isRecoveredSwapFile : Boolean = false) {
    lock synchronized {
      // need to do this in two phases to be crash safe AND do the delete asynchronously
      // if we crash in the middle of this we complete the swap in loadSegments()
      if (!isRecoveredSwapFile)
        newSegment.changeFileSuffixes(Log.CleanedFileSuffix, Log.SwapFileSuffix)
      addSegment(newSegment)

      // delete the old files
      for(seg <- oldSegments) {
        // remove the index entry
        if(seg.baseOffset != newSegment.baseOffset)
          segments.remove(seg.baseOffset)
        // delete segment
        asyncDeleteSegment(seg)
      }
      // okay we are safe now, remove the swap suffix
      newSegment.changeFileSuffixes(Log.SwapFileSuffix, "")
    }
  }

  /**
   * remove deleted log metrics
   */
  private[log] def removeLogMetrics(): Unit = {
    removeMetric("NumLogSegments", tags)
    removeMetric("LogStartOffset", tags)
    removeMetric("LogEndOffset", tags)
    removeMetric("Size", tags)
  }
  /**
   * Add the given segment to the segments in this log. If this segment replaces an existing segment, delete it.
   *
   * @param segment The segment to add
   * 添加日志分段到日志中
   */
  def addSegment(segment: LogSegment) = this.segments.put(segment.baseOffset, segment)

}

/**
 * Helper functions for logs
 */
object Log {

  /** a log file */
  val LogFileSuffix = ".log"

  /** an index file */
  val IndexFileSuffix = ".index"

  /** a time index file */
  val TimeIndexFileSuffix = ".timeindex"

  /** a file that is scheduled to be deleted */
  val DeletedFileSuffix = ".deleted"

  /** A temporary file that is being used for log cleaning */
  val CleanedFileSuffix = ".cleaned"

  /** A temporary file used when swapping files into the log */
  val SwapFileSuffix = ".swap"

  /** Clean shutdown file that indicates the broker was cleanly shutdown in 0.8. This is required to maintain backwards compatibility
   * with 0.8 and avoid unnecessary log recovery when upgrading from 0.8 to 0.8.1 */
  /** TODO: Get rid of CleanShutdownFile in 0.8.2 */
  val CleanShutdownFile = ".kafka_cleanshutdown"

  /** a directory that is scheduled to be deleted */
  val DeleteDirSuffix = "-delete"

  /**
   * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
   * so that ls sorts the files numerically.
   *
   * @param offset The offset to use in the file name
   * @return The filename
   */
  def filenamePrefixFromOffset(offset: Long): String = {
    val nf = NumberFormat.getInstance()
    nf.setMinimumIntegerDigits(20)
    nf.setMaximumFractionDigits(0)
    nf.setGroupingUsed(false)
    nf.format(offset)
  }

  /**
   * Construct a log file name in the given dir with the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def logFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + LogFileSuffix)

  /**
   * Construct an index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def indexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + IndexFileSuffix)

  /**
   * Construct a time index file name in the given dir using the given base offset
   *
   * @param dir The directory in which the log will reside
   * @param offset The base offset of the log file
   */
  def timeIndexFilename(dir: File, offset: Long) =
    new File(dir, filenamePrefixFromOffset(offset) + TimeIndexFileSuffix)

  /**
   * Parse the topic and partition out of the directory name of a log
   */
  def parseTopicPartitionName(dir: File): TopicPartition = {

    def exception(dir: File): KafkaException = {
      new KafkaException("Found directory " + dir.getCanonicalPath + ", " +
        "'" + dir.getName + "' is not in the form of topic-partition\n" +
        "If a directory does not contain Kafka topic data it should not exist in Kafka's log " +
        "directory")
    }

    val dirName = dir.getName
    if (dirName == null || dirName.isEmpty || !dirName.contains('-'))
      throw exception(dir)

    val name: String =
      if (dirName.endsWith(DeleteDirSuffix)) dirName.substring(0, dirName.indexOf('.'))
      else dirName

    val index = name.lastIndexOf('-')
    val topic = name.substring(0, index)
    val partition = name.substring(index + 1)
    if (topic.length < 1 || partition.length < 1)
      throw exception(dir)

    new TopicPartition(topic, partition.toInt)
  }

}

