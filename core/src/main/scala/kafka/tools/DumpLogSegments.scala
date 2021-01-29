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

package kafka.tools

import java.io._
import java.nio.ByteBuffer

import joptsimple.OptionParser
import kafka.coordinator.{GroupMetadataKey, GroupMetadataManager, OffsetKey}
import kafka.log._
import kafka.serializer.Decoder
import kafka.utils._
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.record.{CompressionType, FileRecords, LogEntry, Record}
import org.apache.kafka.common.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

object DumpLogSegments {

  def main(args: Array[String]) {
    val parser = new OptionParser
    val printOpt = parser.accepts("print-data-log", "if set, printing the messages content when dumping data logs. Automatically set if any decoder option is specified.")
    val verifyOpt = parser.accepts("verify-index-only", "if set, just verify the index log without printing its content.")
    val indexSanityOpt = parser.accepts("index-sanity-check", "if set, just checks the index sanity without printing its content. " +
      "This is the same check that is executed on broker startup to determine if an index needs rebuilding or not.")
    val filesOpt = parser.accepts("files", "REQUIRED: The comma separated list of data and index log files to be dumped.")
                           .withRequiredArg
                           .describedAs("file1, file2, ...")
                           .ofType(classOf[String])
    val maxMessageSizeOpt = parser.accepts("max-message-size", "Size of largest message.")
                                  .withRequiredArg
                                  .describedAs("size")
                                  .ofType(classOf[java.lang.Integer])
                                  .defaultsTo(5 * 1024 * 1024)
    val deepIterationOpt = parser.accepts("deep-iteration", "if set, uses deep instead of shallow iteration.")
    val valueDecoderOpt = parser.accepts("value-decoder-class", "if set, used to deserialize the messages. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
                               .withOptionalArg()
                               .ofType(classOf[java.lang.String])
                               .defaultsTo("kafka.serializer.StringDecoder")
    val keyDecoderOpt = parser.accepts("key-decoder-class", "if set, used to deserialize the keys. This class should implement kafka.serializer.Decoder trait. Custom jar should be available in kafka/libs directory.")
                               .withOptionalArg()
                               .ofType(classOf[java.lang.String])
                               .defaultsTo("kafka.serializer.StringDecoder")
    val offsetsOpt = parser.accepts("offsets-decoder", "if set, log data will be parsed as offset data from __consumer_offsets topic.")


    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Parse a log file and dump its contents to the console, useful for debugging a seemingly corrupt log segment.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, filesOpt)

    val printDataLog = options.has(printOpt) || options.has(offsetsOpt) || options.has(valueDecoderOpt) || options.has(keyDecoderOpt)
    val verifyOnly = options.has(verifyOpt)
    val indexSanityOnly = options.has(indexSanityOpt)

    val files = options.valueOf(filesOpt).split(",")
    val maxMessageSize = options.valueOf(maxMessageSizeOpt).intValue()
    val isDeepIteration = options.has(deepIterationOpt)

    val messageParser = if (options.has(offsetsOpt)) {
      new OffsetsMessageParser
    } else {
      val valueDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(valueDecoderOpt), new VerifiableProperties)
      val keyDecoder: Decoder[_] = CoreUtils.createObject[Decoder[_]](options.valueOf(keyDecoderOpt), new VerifiableProperties)
      new DecoderMessageParser(keyDecoder, valueDecoder)
    }

    val misMatchesForIndexFilesMap = new mutable.HashMap[String, List[(Long, Long)]]
    val timeIndexDumpErrors = new TimeIndexDumpErrors
    val nonConsecutivePairsForLogFilesMap = new mutable.HashMap[String, List[(Long, Long)]]

    for(arg <- files) {
      val file = new File(arg)
      if(file.getName.endsWith(Log.LogFileSuffix)) {
        println("Dumping " + file)
        dumpLog(file, printDataLog, nonConsecutivePairsForLogFilesMap, isDeepIteration, maxMessageSize , messageParser)
      } else if(file.getName.endsWith(Log.IndexFileSuffix)) {
        println("Dumping " + file)
        dumpIndex(file, indexSanityOnly, verifyOnly, misMatchesForIndexFilesMap, maxMessageSize)
      } else if(file.getName.endsWith(Log.TimeIndexFileSuffix)) {
        println("Dumping " + file)
        dumpTimeIndex(file, indexSanityOnly, verifyOnly, timeIndexDumpErrors, maxMessageSize)
      }
    }

    misMatchesForIndexFilesMap.foreach {
      case (fileName, listOfMismatches) => {
        System.err.println("Mismatches in :" + fileName)
        listOfMismatches.foreach(m => {
          System.err.println("  Index offset: %d, log offset: %d".format(m._1, m._2))
        })
      }
    }

    timeIndexDumpErrors.printErrors()

    nonConsecutivePairsForLogFilesMap.foreach {
      case (fileName, listOfNonConsecutivePairs) => {
        System.err.println("Non-secutive offsets in :" + fileName)
        listOfNonConsecutivePairs.foreach(m => {
          System.err.println("  %d is followed by %d".format(m._1, m._2))
        })
      }
    }
  }

  /* print out the contents of the index */
  private def dumpIndex(file: File,
                        indexSanityOnly: Boolean,
                        verifyOnly: Boolean,
                        misMatchesForIndexFilesMap: mutable.HashMap[String, List[(Long, Long)]],
                        maxMessageSize: Int) {
    val startOffset = file.getName().split("\\.")(0).toLong
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    val index = new OffsetIndex(file, baseOffset = startOffset)

    //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
    if (indexSanityOnly) {
      index.sanityCheck
      println(s"$file passed sanity check.")
      return
    }

    for(i <- 0 until index.entries) {
      val entry = index.entry(i)
      val slice = fileRecords.read(entry.position, maxMessageSize)
      val logEntry = getIterator(slice.shallowEntries.iterator.next(), isDeepIteration = true).next()
      if (logEntry.offset != entry.offset + index.baseOffset) {
        var misMatchesSeq = misMatchesForIndexFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
        misMatchesSeq ::=(entry.offset + index.baseOffset, logEntry.offset)
        misMatchesForIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      }
      // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
      if(entry.offset == 0 && i > 0)
        return
      if (!verifyOnly)
        println("offset: %d position: %d".format(entry.offset + index.baseOffset, entry.position))
    }
  }

  private def dumpTimeIndex(file: File,
                            indexSanityOnly: Boolean,
                            verifyOnly: Boolean,
                            timeIndexDumpErrors: TimeIndexDumpErrors,
                            maxMessageSize: Int) {
    val startOffset = file.getName.split("\\.")(0).toLong
    val logFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.LogFileSuffix)
    val fileRecords = FileRecords.open(logFile, false)
    val indexFile = new File(file.getAbsoluteFile.getParent, file.getName.split("\\.")(0) + Log.IndexFileSuffix)
    val index = new OffsetIndex(indexFile, baseOffset = startOffset)
    val timeIndex = new TimeIndex(file, baseOffset = startOffset)

    //Check that index passes sanityCheck, this is the check that determines if indexes will be rebuilt on startup or not.
    if (indexSanityOnly) {
      timeIndex.sanityCheck
      println(s"$file passed sanity check.")
      return
    }

    var prevTimestamp = Record.NO_TIMESTAMP
    for(i <- 0 until timeIndex.entries) {
      val entry = timeIndex.entry(i)
      val position = index.lookup(entry.offset + timeIndex.baseOffset).position
      val partialFileRecords = fileRecords.read(position, Int.MaxValue)
      val shallowEntries = partialFileRecords.shallowEntries.asScala
      var maxTimestamp = Record.NO_TIMESTAMP
      // We first find the message by offset then check if the timestamp is correct.
      val maybeLogEntry = shallowEntries.find(_.offset >= entry.offset + timeIndex.baseOffset)
      maybeLogEntry match {
        case None =>
          timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset + timeIndex.baseOffset,
            -1.toLong)
        case Some(logEntry) if logEntry.offset != entry.offset + timeIndex.baseOffset =>
          timeIndexDumpErrors.recordShallowOffsetNotFound(file, entry.offset + timeIndex.baseOffset,
            logEntry.offset)
        case Some(shallowLogEntry) =>
          val deepIter = getIterator(shallowLogEntry, isDeepIteration = true)
          for (deepLogEntry <- deepIter)
            maxTimestamp = math.max(maxTimestamp, deepLogEntry.record.timestamp)

          if (maxTimestamp != entry.timestamp)
            timeIndexDumpErrors.recordMismatchTimeIndex(file, entry.timestamp, maxTimestamp)

          if (prevTimestamp >= entry.timestamp)
            timeIndexDumpErrors.recordOutOfOrderIndexTimestamp(file, entry.timestamp, prevTimestamp)

          // since it is a sparse file, in the event of a crash there may be many zero entries, stop if we see one
          if (entry.offset == 0 && i > 0)
            return
      }
      if (!verifyOnly)
        println("timestamp: %s offset: %s".format(entry.timestamp, timeIndex.baseOffset + entry.offset))
      prevTimestamp = entry.timestamp
    }
  }

  private trait MessageParser[K, V] {
    def parse(record: Record): (Option[K], Option[V])
  }

  private class DecoderMessageParser[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]) extends MessageParser[K, V] {
    override def parse(record: Record): (Option[K], Option[V]) = {
      if (record.hasNullValue) {
        (None, None)
      } else {
        val key = if (record.hasKey)
          Some(keyDecoder.fromBytes(Utils.readBytes(record.key)))
        else
          None

        val payload = Some(valueDecoder.fromBytes(Utils.readBytes(record.value)))

        (key, payload)
      }
    }
  }

  private class OffsetsMessageParser extends MessageParser[String, String] {
    private def hex(bytes: Array[Byte]): String = {
      if (bytes.isEmpty)
        ""
      else
        "%X".format(BigInt(1, bytes))
    }

    private def parseOffsets(offsetKey: OffsetKey, payload: ByteBuffer) = {
      val group = offsetKey.key.group
      val topicPartition = offsetKey.key.topicPartition
      val offset = GroupMetadataManager.readOffsetMessageValue(payload)

      val keyString = s"offset::$group:${topicPartition.topic}:${topicPartition.partition}"
      val valueString = if (offset.metadata.isEmpty)
        String.valueOf(offset.offset)
      else
        s"${offset.offset}:${offset.metadata}"

      (Some(keyString), Some(valueString))
    }

    private def parseGroupMetadata(groupMetadataKey: GroupMetadataKey, payload: ByteBuffer) = {
      val groupId = groupMetadataKey.key
      val group = GroupMetadataManager.readGroupMessageValue(groupId, payload)
      val protocolType = group.protocolType.getOrElse("")

      val assignment = group.allMemberMetadata.map { member =>
        if (protocolType == ConsumerProtocol.PROTOCOL_TYPE) {
          val partitionAssignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment))
          val userData = hex(Utils.toArray(partitionAssignment.userData()))

          if (userData.isEmpty)
            s"${member.memberId}=${partitionAssignment.partitions()}"
          else
            s"${member.memberId}=${partitionAssignment.partitions()}:$userData"
        } else {
          s"${member.memberId}=${hex(member.assignment)}"
        }
      }.mkString("{", ",", "}")

      val keyString = s"metadata::$groupId"
      val valueString = s"$protocolType:${group.protocol}:${group.generationId}:$assignment"

      (Some(keyString), Some(valueString))
    }

    override def parse(record: Record): (Option[String], Option[String]) = {
      if (record.hasNullValue)
        (None, None)
      else if (!record.hasKey) {
        throw new KafkaException("Failed to decode message using offset topic decoder (message had a missing key)")
      } else {
        GroupMetadataManager.readMessageKey(record.key) match {
          case offsetKey: OffsetKey => parseOffsets(offsetKey, record.value)
          case groupMetadataKey: GroupMetadataKey => parseGroupMetadata(groupMetadataKey, record.value)
          case _ => throw new KafkaException("Failed to decode message using offset topic decoder (message had an invalid key)")
        }
      }
    }
  }

  /* print out the contents of the log */
  private def dumpLog(file: File,
                      printContents: Boolean,
                      nonConsecutivePairsForLogFilesMap: mutable.HashMap[String, List[(Long, Long)]],
                      isDeepIteration: Boolean,
                      maxMessageSize: Int,
                      parser: MessageParser[_, _]) {
    val startOffset = file.getName().split("\\.")(0).toLong
    println("Starting offset: " + startOffset)
    val messageSet = FileRecords.open(file, false)
    var validBytes = 0L
    var lastOffset = -1l
    for (shallowLogEntry <- messageSet.shallowEntries(maxMessageSize).asScala) {
      val itr = getIterator(shallowLogEntry, isDeepIteration)
      for (deepLogEntry <- itr) {
        val record = deepLogEntry.record()

        if(lastOffset == -1)
          lastOffset = deepLogEntry.offset
        // If we are iterating uncompressed messages, offsets must be consecutive
        else if (record.compressionType == CompressionType.NONE && deepLogEntry.offset != lastOffset +1) {
          var nonConsecutivePairsSeq = nonConsecutivePairsForLogFilesMap.getOrElse(file.getAbsolutePath, List[(Long, Long)]())
          nonConsecutivePairsSeq ::=(lastOffset, deepLogEntry.offset)
          nonConsecutivePairsForLogFilesMap.put(file.getAbsolutePath, nonConsecutivePairsSeq)
        }
        lastOffset = deepLogEntry.offset

        print("offset: " + deepLogEntry.offset + " position: " + validBytes +
              " " + record.timestampType + ": " + record.timestamp + " isvalid: " + record.isValid +
              " payloadsize: " + record.valueSize + " magic: " + record.magic +
              " compresscodec: " + record.compressionType + " crc: " + record.checksum)
        if (record.hasKey)
          print(" keysize: " + record.keySize)
        if (printContents) {
          val (key, payload) = parser.parse(record)
          key.foreach(key => print(s" key: $key"))
          payload.foreach(payload => print(s" payload: $payload"))
        }
        println()
      }

      validBytes += shallowLogEntry.sizeInBytes
    }
    val trailingBytes = messageSet.sizeInBytes - validBytes
    if(trailingBytes > 0)
      println("Found %d invalid bytes at the end of %s".format(trailingBytes, file.getName))
  }

  private def getIterator(logEntry: LogEntry, isDeepIteration: Boolean): Iterator[LogEntry] = {
    if (isDeepIteration)
      logEntry.iterator.asScala
    else
      Iterator(logEntry)
  }

  class TimeIndexDumpErrors {
    val misMatchesForTimeIndexFilesMap = new mutable.HashMap[String, ArrayBuffer[(Long, Long)]]
    val outOfOrderTimestamp = new mutable.HashMap[String, ArrayBuffer[(Long, Long)]]
    val shallowOffsetNotFound = new mutable.HashMap[String, ArrayBuffer[(Long, Long)]]

    def recordMismatchTimeIndex(file: File, indexTimestamp: Long, logTimestamp: Long) {
      var misMatchesSeq = misMatchesForTimeIndexFilesMap.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (misMatchesSeq.isEmpty)
        misMatchesForTimeIndexFilesMap.put(file.getAbsolutePath, misMatchesSeq)
      misMatchesSeq += ((indexTimestamp, logTimestamp))
    }

    def recordOutOfOrderIndexTimestamp(file: File, indexTimestamp: Long, prevIndexTimestamp: Long) {
      var outOfOrderSeq = outOfOrderTimestamp.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (outOfOrderSeq.isEmpty)
        outOfOrderTimestamp.put(file.getAbsolutePath, outOfOrderSeq)
      outOfOrderSeq += ((indexTimestamp, prevIndexTimestamp))
    }

    def recordShallowOffsetNotFound(file: File, indexOffset: Long, logOffset: Long) {
      var shallowOffsetNotFoundSeq = shallowOffsetNotFound.getOrElse(file.getAbsolutePath, new ArrayBuffer[(Long, Long)]())
      if (shallowOffsetNotFoundSeq.isEmpty)
        shallowOffsetNotFound.put(file.getAbsolutePath, shallowOffsetNotFoundSeq)
      shallowOffsetNotFoundSeq += ((indexOffset, logOffset))
    }

    def printErrors() {
      misMatchesForTimeIndexFilesMap.foreach {
        case (fileName, listOfMismatches) => {
          System.err.println("Found timestamp mismatch in :" + fileName)
          listOfMismatches.foreach(m => {
            System.err.println("  Index timestamp: %d, log timestamp: %d".format(m._1, m._2))
          })
        }
      }

      outOfOrderTimestamp.foreach {
        case (fileName, outOfOrderTimestamps) => {
          System.err.println("Found out of order timestamp in :" + fileName)
          outOfOrderTimestamps.foreach(m => {
            System.err.println("  Index timestamp: %d, Previously indexed timestamp: %d".format(m._1, m._2))
          })
        }
      }

      shallowOffsetNotFound.values.foreach { listOfShallowOffsetNotFound =>
        System.err.println("The following indexed offsets are not found in the log.")
        listOfShallowOffsetNotFound.foreach { case (indexedOffset, logOffset) =>
          System.err.println(s"Indexed offset: $indexedOffset, found log offset: $logOffset")
        }
      }
    }
  }

}
