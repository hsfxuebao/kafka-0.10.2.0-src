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

package kafka.coordinator

import java.io.PrintStream
import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.api.{ApiVersion, KAFKA_0_10_1_IV0}
import kafka.common.{MessageFormatter, _}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.ReplicaManager
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.types.Type._
import org.apache.kafka.common.protocol.types.{ArrayOf, Field, Schema, Struct}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests.OffsetFetchResponse
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection.JavaConverters._
import scala.collection._

class GroupMetadataManager(val brokerId: Int,
                           val interBrokerProtocolVersion: ApiVersion,
                           val config: OffsetConfig,
                           replicaManager: ReplicaManager,
                           zkUtils: ZkUtils,
                           time: Time) extends Logging with KafkaMetricsGroup {

  private val compressionType: CompressionType = CompressionType.forId(config.offsetsTopicCompressionCodec.codec)

  private val groupMetadataCache = new Pool[String, GroupMetadata]

  /* lock protecting access to loading and owned partition sets */
  private val partitionLock = new ReentrantLock()

  /* partitions of consumer groups that are being loaded, its lock should be always called BEFORE the group lock if needed */
  private val loadingPartitions: mutable.Set[Int] = mutable.Set()

  /* partitions of consumer groups that are assigned, using the same loading partition lock */
  private val ownedPartitions: mutable.Set[Int] = mutable.Set()

  /* shutting down flag */
  private val shuttingDown = new AtomicBoolean(false)

  /* number of partitions for the consumer metadata topic */
  private val groupMetadataTopicPartitionCount = getOffsetsTopicPartitionCount

  /* single-thread scheduler to handle offset/group metadata cache loading and unloading */
  private val scheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "group-metadata-manager-")

  this.logIdent = "[Group Metadata Manager on Broker " + brokerId + "]: "

  newGauge("NumOffsets",
    new Gauge[Int] {
      def value = groupMetadataCache.values.map(group => {
        group synchronized { group.numOffsets }
      }).sum
    }
  )

  newGauge("NumGroups",
    new Gauge[Int] {
      def value = groupMetadataCache.size
    }
  )

  def enableMetadataExpiration() {
    scheduler.startup()

    scheduler.schedule(name = "delete-expired-group-metadata",
      fun = cleanupGroupMetadata,
      period = config.offsetsRetentionCheckIntervalMs,
      unit = TimeUnit.MILLISECONDS)
  }

  def currentGroups: Iterable[GroupMetadata] = groupMetadataCache.values

  def isPartitionOwned(partition: Int) = inLock(partitionLock) { ownedPartitions.contains(partition) }

  def isPartitionLoading(partition: Int) = inLock(partitionLock) { loadingPartitions.contains(partition) }

  def partitionFor(groupId: String): Int = Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount

  def isGroupLocal(groupId: String): Boolean = isPartitionOwned(partitionFor(groupId))

  def isGroupLoading(groupId: String): Boolean = isPartitionLoading(partitionFor(groupId))

  def isLoading(): Boolean = inLock(partitionLock) { loadingPartitions.nonEmpty }

  /**
   * Get the group associated with the given groupId, or null if not found
   */
  def getGroup(groupId: String): Option[GroupMetadata] = {
    Option(groupMetadataCache.get(groupId))
  }

  /**
   * Add a group or get the group associated with the given groupId if it already exists
   */
  def addGroup(group: GroupMetadata): GroupMetadata = {
    val currentGroup = groupMetadataCache.putIfNotExists(group.groupId, group)
    if (currentGroup != null) {
      currentGroup
    } else {
      group
    }
  }

  def prepareStoreGroup(group: GroupMetadata,
                        groupAssignment: Map[String, Array[Byte]],
                        responseCallback: Errors => Unit): Option[DelayedStore] = {
    getMagicAndTimestamp(partitionFor(group.groupId)) match {
      case Some((magicValue, timestampType, timestamp)) =>
        val groupMetadataValueVersion = {
          if (interBrokerProtocolVersion < KAFKA_0_10_1_IV0)
            0.toShort
          else
            GroupMetadataManager.CURRENT_GROUP_VALUE_SCHEMA_VERSION
        }

        val record = Record.create(magicValue, timestampType, timestamp,
          GroupMetadataManager.groupMetadataKey(group.groupId),
          GroupMetadataManager.groupMetadataValue(group, groupAssignment, version = groupMetadataValueVersion))

        val groupMetadataPartition = new TopicPartition(Topic.GroupMetadataTopicName, partitionFor(group.groupId))
        val groupMetadataRecords = Map(groupMetadataPartition -> MemoryRecords.withRecords(timestampType, compressionType, record))
        val generationId = group.generationId

        // set the callback function to insert the created group into cache after log append completed
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
          // the append response should only contain the topics partition
          if (responseStatus.size != 1 || !responseStatus.contains(groupMetadataPartition))
            throw new IllegalStateException("Append status %s should only have one partition %s"
              .format(responseStatus, groupMetadataPartition))

          // construct the error status in the propagated assignment response
          // in the cache
          val status = responseStatus(groupMetadataPartition)

          val responseError = if (status.error == Errors.NONE) {
            Errors.NONE
          } else {
            debug(s"Metadata from group ${group.groupId} with generation $generationId failed when appending to log " +
              s"due to ${status.error.exceptionName}")

            // transform the log append error code to the corresponding the commit status error code
            status.error match {
              case Errors.UNKNOWN_TOPIC_OR_PARTITION
                   | Errors.NOT_ENOUGH_REPLICAS
                   | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                Errors.GROUP_COORDINATOR_NOT_AVAILABLE

              case Errors.NOT_LEADER_FOR_PARTITION =>
                Errors.NOT_COORDINATOR_FOR_GROUP

              case Errors.REQUEST_TIMED_OUT =>
                Errors.REBALANCE_IN_PROGRESS

              case Errors.MESSAGE_TOO_LARGE
                   | Errors.RECORD_LIST_TOO_LARGE
                   | Errors.INVALID_FETCH_SIZE =>

                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed due to " +
                  s"${status.error.exceptionName}, returning UNKNOWN error code to the client")

                Errors.UNKNOWN

              case other =>
                error(s"Appending metadata message for group ${group.groupId} generation $generationId failed " +
                  s"due to unexpected error: ${status.error.exceptionName}")

                other
            }
          }

          responseCallback(responseError)
        }
        Some(DelayedStore(groupMetadataRecords, putCacheCallback))

      case None =>
        responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP)
        None
    }
  }

  def store(delayedStore: DelayedStore) {
    // call replica manager to append the group message
    replicaManager.appendRecords(
      config.offsetCommitTimeoutMs.toLong,
      config.offsetCommitRequiredAcks,
      true, // allow appending to internal offset topic
      delayedStore.partitionRecords,
      delayedStore.callback)
  }

  /**
   * Store offsets by appending it to the replicated log and then inserting to cache
   */
  def prepareStoreOffsets(group: GroupMetadata,
                          consumerId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit): Option[DelayedStore] = {
    // first filter out partitions with offset metadata size exceeding limit
    val filteredOffsetMetadata = offsetMetadata.filter { case (_, offsetAndMetadata) =>
      validateOffsetMetadataLength(offsetAndMetadata.metadata)
    }

    // construct the message set to append
    getMagicAndTimestamp(partitionFor(group.groupId)) match {
      case Some((magicValue, timestampType, timestamp)) =>
        val records = filteredOffsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
          Record.create(magicValue, timestampType, timestamp,
            GroupMetadataManager.offsetCommitKey(group.groupId, topicPartition),
            GroupMetadataManager.offsetCommitValue(offsetAndMetadata))
        }.toSeq

        val offsetTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, partitionFor(group.groupId))

        val entries = Map(offsetTopicPartition -> MemoryRecords.withRecords(timestampType, compressionType, records:_*))

        // set the callback function to insert offsets into cache after log append completed
        def putCacheCallback(responseStatus: Map[TopicPartition, PartitionResponse]) {
          // the append response should only contain the topics partition
          if (responseStatus.size != 1 || ! responseStatus.contains(offsetTopicPartition))
            throw new IllegalStateException("Append status %s should only have one partition %s"
              .format(responseStatus, offsetTopicPartition))

          // construct the commit response status and insert
          // the offset and metadata to cache if the append status has no error
          val status = responseStatus(offsetTopicPartition)

          val responseCode =
            group synchronized {
              if (status.error == Errors.NONE) {
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    group.completePendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }
                Errors.NONE.code
              } else {
                if (!group.is(Dead)) {
                  filteredOffsetMetadata.foreach { case (topicPartition, offsetAndMetadata) =>
                    group.failPendingOffsetWrite(topicPartition, offsetAndMetadata)
                  }
                }

                debug(s"Offset commit $filteredOffsetMetadata from group ${group.groupId}, consumer $consumerId " +
                  s"with generation $generationId failed when appending to log due to ${status.error.exceptionName}")

                // transform the log append error code to the corresponding the commit status error code
                val responseError = status.error match {
                  case Errors.UNKNOWN_TOPIC_OR_PARTITION
                       | Errors.NOT_ENOUGH_REPLICAS
                       | Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND =>
                    Errors.GROUP_COORDINATOR_NOT_AVAILABLE

                  case Errors.NOT_LEADER_FOR_PARTITION =>
                    Errors.NOT_COORDINATOR_FOR_GROUP

                  case Errors.MESSAGE_TOO_LARGE
                       | Errors.RECORD_LIST_TOO_LARGE
                       | Errors.INVALID_FETCH_SIZE =>
                    Errors.INVALID_COMMIT_OFFSET_SIZE

                  case other => other
                }

                responseError.code
              }
            }

          // compute the final error codes for the commit response
          val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
            if (validateOffsetMetadataLength(offsetAndMetadata.metadata))
              (topicPartition, responseCode)
            else
              (topicPartition, Errors.OFFSET_METADATA_TOO_LARGE.code)
          }

          // finally trigger the callback logic passed from the API layer
          responseCallback(commitStatus)
        }

        group synchronized {
          group.prepareOffsetCommit(offsetMetadata)
        }

        Some(DelayedStore(entries, putCacheCallback))

      case None =>
        val commitStatus = offsetMetadata.map { case (topicPartition, offsetAndMetadata) =>
          (topicPartition, Errors.NOT_COORDINATOR_FOR_GROUP.code)
        }
        responseCallback(commitStatus)
        None
    }
  }

  /**
   * The most important guarantee that this API provides is that it should never return a stale offset. i.e., it either
   * returns the current offset or it begins to sync the cache from the log (and returns an error code).
   */
  def getOffsets(groupId: String, topicPartitionsOpt: Option[Seq[TopicPartition]]): Map[TopicPartition, OffsetFetchResponse.PartitionData] = {
    trace("Getting offsets of %s for group %s.".format(topicPartitionsOpt.getOrElse("all partitions"), groupId))
    val group = groupMetadataCache.get(groupId)
    if (group == null) {
      topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
        (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE))
      }.toMap
    } else {
      group synchronized {
        if (group.is(Dead)) {
          topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
            (topicPartition, new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE))
          }.toMap
        } else {
            topicPartitionsOpt match {
              case None =>
                // Return offsets for all partitions owned by this consumer group. (this only applies to consumers
                // that commit offsets to Kafka.)
                group.allOffsets.map { case (topicPartition, offsetAndMetadata) =>
                  topicPartition -> new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE)
                }

              case Some(topicPartitions) =>
                topicPartitionsOpt.getOrElse(Seq.empty[TopicPartition]).map { topicPartition =>
                  val partitionData = group.offset(topicPartition) match {
                    case None =>
                      new OffsetFetchResponse.PartitionData(OffsetFetchResponse.INVALID_OFFSET, "", Errors.NONE)
                    case Some(offsetAndMetadata) =>
                      new OffsetFetchResponse.PartitionData(offsetAndMetadata.offset, offsetAndMetadata.metadata, Errors.NONE)
                  }
                  topicPartition -> partitionData
                }.toMap
            }
        }
      }
    }
  }

  /**
   * Asynchronously read the partition from the offsets topic and populate the cache
   */
  def loadGroupsForPartition(offsetsPartition: Int, onGroupLoaded: GroupMetadata => Unit) {
    val topicPartition = new TopicPartition(Topic.GroupMetadataTopicName, offsetsPartition)

    def doLoadGroupsAndOffsets() {
      info(s"Loading offsets and group metadata from $topicPartition")

      inLock(partitionLock) {
        if (loadingPartitions.contains(offsetsPartition)) {
          info(s"Offset load from $topicPartition already in progress.")
          return
        } else {
          loadingPartitions.add(offsetsPartition)
        }
      }

      try {
        loadGroupsAndOffsets(topicPartition, onGroupLoaded)
      } catch {
        case t: Throwable => error(s"Error loading offsets from $topicPartition", t)
      } finally {
        inLock(partitionLock) {
          ownedPartitions.add(offsetsPartition)
          loadingPartitions.remove(offsetsPartition)
        }
      }
    }

    scheduler.schedule(topicPartition.toString, doLoadGroupsAndOffsets)
  }

  private[coordinator] def loadGroupsAndOffsets(topicPartition: TopicPartition, onGroupLoaded: GroupMetadata => Unit) {
    def highWaterMark = replicaManager.getHighWatermark(topicPartition).getOrElse(-1L)

    val startMs = time.milliseconds()
    replicaManager.getLog(topicPartition) match {
      case None =>
        warn(s"Attempted to load offsets and group metadata from $topicPartition, but found no log")

      case Some(log) =>
        var currOffset = log.logStartOffset
        val buffer = ByteBuffer.allocate(config.loadBufferSize)
        // loop breaks if leader changes at any time during the load, since getHighWatermark is -1
        val loadedOffsets = mutable.Map[GroupTopicPartition, OffsetAndMetadata]()
        val removedOffsets = mutable.Set[GroupTopicPartition]()
        val loadedGroups = mutable.Map[String, GroupMetadata]()
        val removedGroups = mutable.Set[String]()

        while (currOffset < highWaterMark && !shuttingDown.get()) {
          buffer.clear()
          val fileRecords = log.read(currOffset, config.loadBufferSize, maxOffset = None, minOneMessage = true)
            .records.asInstanceOf[FileRecords]
          val bufferRead = fileRecords.readInto(buffer, 0)

          MemoryRecords.readableRecords(bufferRead).deepEntries.asScala.foreach { entry =>
            val record = entry.record
            require(record.hasKey, "Group metadata/offset entry key should not be null")

            GroupMetadataManager.readMessageKey(record.key) match {
              case offsetKey: OffsetKey =>
                // load offset
                val key = offsetKey.key
                if (record.hasNullValue) {
                  loadedOffsets.remove(key)
                  removedOffsets.add(key)
                } else {
                  val value = GroupMetadataManager.readOffsetMessageValue(record.value)
                  loadedOffsets.put(key, value)
                  removedOffsets.remove(key)
                }

              case groupMetadataKey: GroupMetadataKey =>
                // load group metadata
                val groupId = groupMetadataKey.key
                val groupMetadata = GroupMetadataManager.readGroupMessageValue(groupId, record.value)
                if (groupMetadata != null) {
                  trace(s"Loaded group metadata for group $groupId with generation ${groupMetadata.generationId}")
                  removedGroups.remove(groupId)
                  loadedGroups.put(groupId, groupMetadata)
                } else {
                  loadedGroups.remove(groupId)
                  removedGroups.add(groupId)
                }

              case unknownKey =>
                throw new IllegalStateException(s"Unexpected message key $unknownKey while loading offsets and group metadata")
            }

            currOffset = entry.nextOffset
          }
        }

        val (groupOffsets, emptyGroupOffsets) = loadedOffsets
          .groupBy(_._1.group)
          .mapValues(_.map { case (groupTopicPartition, offset) => (groupTopicPartition.topicPartition, offset)} )
          .partition { case (group, _) => loadedGroups.contains(group) }

        loadedGroups.values.foreach { group =>
          val offsets = groupOffsets.getOrElse(group.groupId, Map.empty[TopicPartition, OffsetAndMetadata])
          loadGroup(group, offsets)
          onGroupLoaded(group)
        }

        // load groups which store offsets in kafka, but which have no active members and thus no group
        // metadata stored in the log
        emptyGroupOffsets.foreach { case (groupId, offsets) =>
          val group = new GroupMetadata(groupId)
          loadGroup(group, offsets)
          onGroupLoaded(group)
        }

        removedGroups.foreach { groupId =>
          // if the cache already contains a group which should be removed, raise an error. Note that it
          // is possible (however unlikely) for a consumer group to be removed, and then to be used only for
          // offset storage (i.e. by "simple" consumers)
          if (groupMetadataCache.contains(groupId) && !emptyGroupOffsets.contains(groupId))
            throw new IllegalStateException(s"Unexpected unload of active group $groupId while " +
              s"loading partition $topicPartition")
        }

        if (!shuttingDown.get())
          info("Finished loading offsets from %s in %d milliseconds."
            .format(topicPartition, time.milliseconds() - startMs))
    }
  }

  private def loadGroup(group: GroupMetadata, offsets: Map[TopicPartition, OffsetAndMetadata]): Unit = {
    // offsets are initialized prior to loading the group into the cache to ensure that clients see a consistent
    // view of the group's offsets
    val loadedOffsets = offsets.mapValues { offsetAndMetadata =>
      // special handling for version 0:
      // set the expiration time stamp as commit time stamp + server default retention time
      if (offsetAndMetadata.expireTimestamp == org.apache.kafka.common.requests.OffsetCommitRequest.DEFAULT_TIMESTAMP)
        offsetAndMetadata.copy(expireTimestamp = offsetAndMetadata.commitTimestamp + config.offsetsRetentionMs)
      else
        offsetAndMetadata
    }
    trace(s"Initialized offsets $loadedOffsets for group ${group.groupId}")
    group.initializeOffsets(loadedOffsets)

    val currentGroup = addGroup(group)
    if (group != currentGroup)
      debug(s"Attempt to load group ${group.groupId} from log with generation ${group.generationId} failed " +
        s"because there is already a cached group with generation ${currentGroup.generationId}")
  }

  /**
   * When this broker becomes a follower for an offsets topic partition clear out the cache for groups that belong to
   * that partition.
   *
   * @param offsetsPartition Groups belonging to this partition of the offsets topic will be deleted from the cache.
   */
  def removeGroupsForPartition(offsetsPartition: Int,
                               onGroupUnloaded: GroupMetadata => Unit) {
    val topicPartition = new TopicPartition(Topic.GroupMetadataTopicName, offsetsPartition)
    scheduler.schedule(topicPartition.toString, removeGroupsAndOffsets)

    def removeGroupsAndOffsets() {
      var numOffsetsRemoved = 0
      var numGroupsRemoved = 0

      inLock(partitionLock) {
        // we need to guard the group removal in cache in the loading partition lock
        // to prevent coordinator's check-and-get-group race condition
        ownedPartitions.remove(offsetsPartition)

        for (group <- groupMetadataCache.values) {
          if (partitionFor(group.groupId) == offsetsPartition) {
            onGroupUnloaded(group)
            groupMetadataCache.remove(group.groupId, group)
            numGroupsRemoved += 1
            numOffsetsRemoved += group.numOffsets
          }
        }
      }

      if (numOffsetsRemoved > 0)
        info(s"Removed $numOffsetsRemoved cached offsets for $topicPartition on follower transition.")

      if (numGroupsRemoved > 0)
        info(s"Removed $numGroupsRemoved cached groups for $topicPartition on follower transition.")
    }
  }

  // visible for testing
  private[coordinator] def cleanupGroupMetadata(): Unit = {
    cleanupGroupMetadata(None)
  }

  def cleanupGroupMetadata(deletedTopicPartitions: Option[Seq[TopicPartition]]) {
    val startMs = time.milliseconds()
    var offsetsRemoved = 0

    groupMetadataCache.foreach { case (groupId, group) =>
      val (removedOffsets, groupIsDead, generation) = group synchronized {
        val removedOffsets = deletedTopicPartitions match {
          case Some(topicPartitions) => group.removeOffsets(topicPartitions)
          case None => group.removeExpiredOffsets(startMs)
        }

        if (group.is(Empty) && !group.hasOffsets) {
          info(s"Group $groupId transitioned to Dead in generation ${group.generationId}")
          group.transitionTo(Dead)
        }
        (removedOffsets, group.is(Dead), group.generationId)
      }

      val offsetsPartition = partitionFor(groupId)
      val appendPartition = new TopicPartition(Topic.GroupMetadataTopicName, offsetsPartition)
      getMagicAndTimestamp(offsetsPartition) match {
        case Some((magicValue, timestampType, timestamp)) =>
          val partitionOpt = replicaManager.getPartition(appendPartition)
          partitionOpt.foreach { partition =>
            val tombstones = removedOffsets.map { case (topicPartition, offsetAndMetadata) =>
              trace(s"Removing expired/deleted offset and metadata for $groupId, $topicPartition: $offsetAndMetadata")
              val commitKey = GroupMetadataManager.offsetCommitKey(groupId, topicPartition)
              Record.create(magicValue, timestampType, timestamp, commitKey, null)
            }.toBuffer
            trace(s"Marked ${removedOffsets.size} offsets in $appendPartition for deletion.")

            // We avoid writing the tombstone when the generationId is 0, since this group is only using
            // Kafka for offset storage.
            if (groupIsDead && groupMetadataCache.remove(groupId, group) && generation > 0) {
              // Append the tombstone messages to the partition. It is okay if the replicas don't receive these (say,
              // if we crash or leaders move) since the new leaders will still expire the consumers with heartbeat and
              // retry removing this group.
              tombstones += Record.create(magicValue, timestampType, timestamp, GroupMetadataManager.groupMetadataKey(group.groupId), null)
              trace(s"Group $groupId removed from the metadata cache and marked for deletion in $appendPartition.")
            }

            if (tombstones.nonEmpty) {
              try {
                // do not need to require acks since even if the tombstone is lost,
                // it will be appended again in the next purge cycle
                partition.appendRecordsToLeader(MemoryRecords.withRecords(timestampType, compressionType, tombstones: _*))
                offsetsRemoved += removedOffsets.size
                trace(s"Successfully appended ${tombstones.size} tombstones to $appendPartition for expired/deleted offsets and/or metadata for group $groupId")
              } catch {
                case t: Throwable =>
                  error(s"Failed to append ${tombstones.size} tombstones to $appendPartition for expired/deleted offsets and/or metadata for group $groupId.", t)
                  // ignore and continue
              }
            }
          }

        case None =>
          info(s"BrokerId $brokerId is no longer a coordinator for the group $groupId. Proceeding cleanup for other alive groups")
      }
    }

    info(s"Removed $offsetsRemoved expired offsets in ${time.milliseconds() - startMs} milliseconds.")
  }

  /*
   * Check if the offset metadata length is valid
   */
  private def validateOffsetMetadataLength(metadata: String) : Boolean = {
    metadata == null || metadata.length() <= config.maxMetadataSize
  }


  def shutdown() {
    shuttingDown.set(true)
    if (scheduler.isStarted)
      scheduler.shutdown()

    // TODO: clear the caches
  }

  /**
   * Gets the partition count of the offsets topic from ZooKeeper.
   * If the topic does not exist, the configured partition count is returned.
   */
  private def getOffsetsTopicPartitionCount = {
    val topic = Topic.GroupMetadataTopicName
    val topicData = zkUtils.getPartitionAssignmentForTopics(Seq(topic))
    if (topicData(topic).nonEmpty)
      topicData(topic).size
    else
      config.offsetsTopicNumPartitions
  }

  /**
   * Check if the replica is local and return the message format version and timestamp
   *
   * @param   partition  Partition of GroupMetadataTopic
   * @return  Option[(MessageFormatVersion, TimeStamp)] if replica is local, None otherwise
   */
  private def getMagicAndTimestamp(partition: Int): Option[(Byte, TimestampType, Long)] = {
    val groupMetadataTopicPartition = new TopicPartition(Topic.GroupMetadataTopicName, partition)
    replicaManager.getMagicAndTimestampType(groupMetadataTopicPartition).map { case (messageFormatVersion, timestampType) =>
      val timestamp = if (messageFormatVersion == Record.MAGIC_VALUE_V0) Record.NO_TIMESTAMP else time.milliseconds()
      (messageFormatVersion, timestampType, timestamp)
    }
  }

  /**
   * Add the partition into the owned list
   *
   * NOTE: this is for test only
   */
  def addPartitionOwnership(partition: Int) {
    inLock(partitionLock) {
      ownedPartitions.add(partition)
    }
  }
}

/**
 * Messages stored for the group topic has versions for both the key and value fields. Key
 * version is used to indicate the type of the message (also to differentiate different types
 * of messages from being compacted together if they have the same field values); and value
 * version is used to evolve the messages within their data types:
 *
 * key version 0:       group consumption offset
 *    -> value version 0:       [offset, metadata, timestamp]
 *
 * key version 1:       group consumption offset
 *    -> value version 1:       [offset, metadata, commit_timestamp, expire_timestamp]
 *
 * key version 2:       group metadata
 *     -> value version 0:       [protocol_type, generation, protocol, leader, members]
 */
object GroupMetadataManager {

  private val CURRENT_OFFSET_KEY_SCHEMA_VERSION = 1.toShort
  private val CURRENT_GROUP_KEY_SCHEMA_VERSION = 2.toShort

  private val OFFSET_COMMIT_KEY_SCHEMA = new Schema(new Field("group", STRING),
    new Field("topic", STRING),
    new Field("partition", INT32))
  private val OFFSET_KEY_GROUP_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("group")
  private val OFFSET_KEY_TOPIC_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("topic")
  private val OFFSET_KEY_PARTITION_FIELD = OFFSET_COMMIT_KEY_SCHEMA.get("partition")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V0 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("metadata")
  private val OFFSET_VALUE_TIMESTAMP_FIELD_V0 = OFFSET_COMMIT_VALUE_SCHEMA_V0.get("timestamp")

  private val OFFSET_COMMIT_VALUE_SCHEMA_V1 = new Schema(new Field("offset", INT64),
    new Field("metadata", STRING, "Associated metadata.", ""),
    new Field("commit_timestamp", INT64),
    new Field("expire_timestamp", INT64))
  private val OFFSET_VALUE_OFFSET_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("offset")
  private val OFFSET_VALUE_METADATA_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("metadata")
  private val OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("commit_timestamp")
  private val OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1 = OFFSET_COMMIT_VALUE_SCHEMA_V1.get("expire_timestamp")

  private val GROUP_METADATA_KEY_SCHEMA = new Schema(new Field("group", STRING))
  private val GROUP_KEY_GROUP_FIELD = GROUP_METADATA_KEY_SCHEMA.get("group")

  private val MEMBER_ID_KEY = "member_id"
  private val CLIENT_ID_KEY = "client_id"
  private val CLIENT_HOST_KEY = "client_host"
  private val REBALANCE_TIMEOUT_KEY = "rebalance_timeout"
  private val SESSION_TIMEOUT_KEY = "session_timeout"
  private val SUBSCRIPTION_KEY = "subscription"
  private val ASSIGNMENT_KEY = "assignment"

  private val MEMBER_METADATA_V0 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val MEMBER_METADATA_V1 = new Schema(
    new Field(MEMBER_ID_KEY, STRING),
    new Field(CLIENT_ID_KEY, STRING),
    new Field(CLIENT_HOST_KEY, STRING),
    new Field(REBALANCE_TIMEOUT_KEY, INT32),
    new Field(SESSION_TIMEOUT_KEY, INT32),
    new Field(SUBSCRIPTION_KEY, BYTES),
    new Field(ASSIGNMENT_KEY, BYTES))

  private val PROTOCOL_TYPE_KEY = "protocol_type"
  private val GENERATION_KEY = "generation"
  private val PROTOCOL_KEY = "protocol"
  private val LEADER_KEY = "leader"
  private val MEMBERS_KEY = "members"

  private val GROUP_METADATA_VALUE_SCHEMA_V0 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V0)))

  private val GROUP_METADATA_VALUE_SCHEMA_V1 = new Schema(
    new Field(PROTOCOL_TYPE_KEY, STRING),
    new Field(GENERATION_KEY, INT32),
    new Field(PROTOCOL_KEY, NULLABLE_STRING),
    new Field(LEADER_KEY, NULLABLE_STRING),
    new Field(MEMBERS_KEY, new ArrayOf(MEMBER_METADATA_V1)))


  // map of versions to key schemas as data types
  private val MESSAGE_TYPE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_KEY_SCHEMA,
    1 -> OFFSET_COMMIT_KEY_SCHEMA,
    2 -> GROUP_METADATA_KEY_SCHEMA)

  // map of version of offset value schemas
  private val OFFSET_VALUE_SCHEMAS = Map(
    0 -> OFFSET_COMMIT_VALUE_SCHEMA_V0,
    1 -> OFFSET_COMMIT_VALUE_SCHEMA_V1)
  private val CURRENT_OFFSET_VALUE_SCHEMA_VERSION = 1.toShort

  // map of version of group metadata value schemas
  private val GROUP_VALUE_SCHEMAS = Map(
    0 -> GROUP_METADATA_VALUE_SCHEMA_V0,
    1 -> GROUP_METADATA_VALUE_SCHEMA_V1)
  private val CURRENT_GROUP_VALUE_SCHEMA_VERSION = 1.toShort

  private val CURRENT_OFFSET_KEY_SCHEMA = schemaForKey(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
  private val CURRENT_GROUP_KEY_SCHEMA = schemaForKey(CURRENT_GROUP_KEY_SCHEMA_VERSION)

  private val CURRENT_OFFSET_VALUE_SCHEMA = schemaForOffset(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
  private val CURRENT_GROUP_VALUE_SCHEMA = schemaForGroup(CURRENT_GROUP_VALUE_SCHEMA_VERSION)

  private def schemaForKey(version: Int) = {
    val schemaOpt = MESSAGE_TYPE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForOffset(version: Int) = {
    val schemaOpt = OFFSET_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown offset schema version " + version)
    }
  }

  private def schemaForGroup(version: Int) = {
    val schemaOpt = GROUP_VALUE_SCHEMAS.get(version)
    schemaOpt match {
      case Some(schema) => schema
      case _ => throw new KafkaException("Unknown group metadata version " + version)
    }
  }

  /**
   * Generates the key for offset commit message for given (group, topic, partition)
   *
   * @return key for offset commit message
   */
  private[coordinator] def offsetCommitKey(group: String, topicPartition: TopicPartition,
                                           versionId: Short = 0): Array[Byte] = {
    val key = new Struct(CURRENT_OFFSET_KEY_SCHEMA)
    key.set(OFFSET_KEY_GROUP_FIELD, group)
    key.set(OFFSET_KEY_TOPIC_FIELD, topicPartition.topic)
    key.set(OFFSET_KEY_PARTITION_FIELD, topicPartition.partition)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the key for group metadata message for given group
   *
   * @return key bytes for group metadata message
   */
  private[coordinator] def groupMetadataKey(group: String): Array[Byte] = {
    val key = new Struct(CURRENT_GROUP_KEY_SCHEMA)
    key.set(GROUP_KEY_GROUP_FIELD, group)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + key.sizeOf)
    byteBuffer.putShort(CURRENT_GROUP_KEY_SCHEMA_VERSION)
    key.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for offset commit message from given offset and metadata
   *
   * @param offsetAndMetadata consumer's current offset and metadata
   * @return payload for offset commit message
   */
  private[coordinator] def offsetCommitValue(offsetAndMetadata: OffsetAndMetadata): Array[Byte] = {
    // generate commit value with schema version 1
    val value = new Struct(CURRENT_OFFSET_VALUE_SCHEMA)
    value.set(OFFSET_VALUE_OFFSET_FIELD_V1, offsetAndMetadata.offset)
    value.set(OFFSET_VALUE_METADATA_FIELD_V1, offsetAndMetadata.metadata)
    value.set(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1, offsetAndMetadata.commitTimestamp)
    value.set(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1, offsetAndMetadata.expireTimestamp)
    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(CURRENT_OFFSET_VALUE_SCHEMA_VERSION)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Generates the payload for group metadata message from given offset and metadata
   * assuming the generation id, selected protocol, leader and member assignment are all available
   *
   * @param groupMetadata current group metadata
   * @param assignment the assignment for the rebalancing generation
   * @param version the version of the value message to use
   * @return payload for offset commit message
   */
  private[coordinator] def groupMetadataValue(groupMetadata: GroupMetadata,
                                              assignment: Map[String, Array[Byte]],
                                              version: Short = 0): Array[Byte] = {
    val value = if (version == 0) new Struct(GROUP_METADATA_VALUE_SCHEMA_V0) else new Struct(CURRENT_GROUP_VALUE_SCHEMA)

    value.set(PROTOCOL_TYPE_KEY, groupMetadata.protocolType.getOrElse(""))
    value.set(GENERATION_KEY, groupMetadata.generationId)
    value.set(PROTOCOL_KEY, groupMetadata.protocol)
    value.set(LEADER_KEY, groupMetadata.leaderId)

    val memberArray = groupMetadata.allMemberMetadata.map { memberMetadata =>
      val memberStruct = value.instance(MEMBERS_KEY)
      memberStruct.set(MEMBER_ID_KEY, memberMetadata.memberId)
      memberStruct.set(CLIENT_ID_KEY, memberMetadata.clientId)
      memberStruct.set(CLIENT_HOST_KEY, memberMetadata.clientHost)
      memberStruct.set(SESSION_TIMEOUT_KEY, memberMetadata.sessionTimeoutMs)

      if (version > 0)
        memberStruct.set(REBALANCE_TIMEOUT_KEY, memberMetadata.rebalanceTimeoutMs)

      val metadata = memberMetadata.metadata(groupMetadata.protocol)
      memberStruct.set(SUBSCRIPTION_KEY, ByteBuffer.wrap(metadata))

      val memberAssignment = assignment(memberMetadata.memberId)
      assert(memberAssignment != null)

      memberStruct.set(ASSIGNMENT_KEY, ByteBuffer.wrap(memberAssignment))

      memberStruct
    }

    value.set(MEMBERS_KEY, memberArray.toArray)

    val byteBuffer = ByteBuffer.allocate(2 /* version */ + value.sizeOf)
    byteBuffer.putShort(version)
    value.writeTo(byteBuffer)
    byteBuffer.array()
  }

  /**
   * Decodes the offset messages' key
   *
   * @param buffer input byte-buffer
   * @return an GroupTopicPartition object
   */
  def readMessageKey(buffer: ByteBuffer): BaseKey = {
    val version = buffer.getShort
    val keySchema = schemaForKey(version)
    val key = keySchema.read(buffer)

    if (version <= CURRENT_OFFSET_KEY_SCHEMA_VERSION) {
      // version 0 and 1 refer to offset
      val group = key.get(OFFSET_KEY_GROUP_FIELD).asInstanceOf[String]
      val topic = key.get(OFFSET_KEY_TOPIC_FIELD).asInstanceOf[String]
      val partition = key.get(OFFSET_KEY_PARTITION_FIELD).asInstanceOf[Int]

      OffsetKey(version, GroupTopicPartition(group, new TopicPartition(topic, partition)))

    } else if (version == CURRENT_GROUP_KEY_SCHEMA_VERSION) {
      // version 2 refers to offset
      val group = key.get(GROUP_KEY_GROUP_FIELD).asInstanceOf[String]

      GroupMetadataKey(version, group)
    } else {
      throw new IllegalStateException("Unknown version " + version + " for group metadata message")
    }
  }

  /**
   * Decodes the offset messages' payload and retrieves offset and metadata from it
   *
   * @param buffer input byte-buffer
   * @return an offset-metadata object from the message
   */
  def readOffsetMessageValue(buffer: ByteBuffer): OffsetAndMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForOffset(version)
      val value = valueSchema.read(buffer)

      if (version == 0) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V0).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V0).asInstanceOf[String]
        val timestamp = value.get(OFFSET_VALUE_TIMESTAMP_FIELD_V0).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, timestamp)
      } else if (version == 1) {
        val offset = value.get(OFFSET_VALUE_OFFSET_FIELD_V1).asInstanceOf[Long]
        val metadata = value.get(OFFSET_VALUE_METADATA_FIELD_V1).asInstanceOf[String]
        val commitTimestamp = value.get(OFFSET_VALUE_COMMIT_TIMESTAMP_FIELD_V1).asInstanceOf[Long]
        val expireTimestamp = value.get(OFFSET_VALUE_EXPIRE_TIMESTAMP_FIELD_V1).asInstanceOf[Long]

        OffsetAndMetadata(offset, metadata, commitTimestamp, expireTimestamp)
      } else {
        throw new IllegalStateException("Unknown offset message version")
      }
    }
  }

  /**
   * Decodes the group metadata messages' payload and retrieves its member metadatafrom it
   *
   * @param buffer input byte-buffer
   * @return a group metadata object from the message
   */
  def readGroupMessageValue(groupId: String, buffer: ByteBuffer): GroupMetadata = {
    if (buffer == null) { // tombstone
      null
    } else {
      val version = buffer.getShort
      val valueSchema = schemaForGroup(version)
      val value = valueSchema.read(buffer)

      if (version == 0 || version == 1) {
        val protocolType = value.get(PROTOCOL_TYPE_KEY).asInstanceOf[String]

        val memberMetadataArray = value.getArray(MEMBERS_KEY)
        val initialState = if (memberMetadataArray.isEmpty) Empty else Stable

        val group = new GroupMetadata(groupId, initialState)

        group.generationId = value.get(GENERATION_KEY).asInstanceOf[Int]
        group.leaderId = value.get(LEADER_KEY).asInstanceOf[String]
        group.protocol = value.get(PROTOCOL_KEY).asInstanceOf[String]

        memberMetadataArray.foreach { memberMetadataObj =>
          val memberMetadata = memberMetadataObj.asInstanceOf[Struct]
          val memberId = memberMetadata.get(MEMBER_ID_KEY).asInstanceOf[String]
          val clientId = memberMetadata.get(CLIENT_ID_KEY).asInstanceOf[String]
          val clientHost = memberMetadata.get(CLIENT_HOST_KEY).asInstanceOf[String]
          val sessionTimeout = memberMetadata.get(SESSION_TIMEOUT_KEY).asInstanceOf[Int]
          val rebalanceTimeout = if (version == 0) sessionTimeout else memberMetadata.get(REBALANCE_TIMEOUT_KEY).asInstanceOf[Int]

          val subscription = Utils.toArray(memberMetadata.get(SUBSCRIPTION_KEY).asInstanceOf[ByteBuffer])

          val member = new MemberMetadata(memberId, groupId, clientId, clientHost, rebalanceTimeout, sessionTimeout,
            protocolType, List((group.protocol, subscription)))

          member.assignment = Utils.toArray(memberMetadata.get(ASSIGNMENT_KEY).asInstanceOf[ByteBuffer])

          group.add(member)
        }

        group
      } else {
        throw new IllegalStateException("Unknown group metadata message version")
      }
    }
  }

  // Formatter for use with tools such as console consumer: Consumer should also set exclude.internal.topics to false.
  // (specify --formatter "kafka.coordinator.GroupMetadataManager\$OffsetsMessageFormatter" when consuming __consumer_offsets)
  class OffsetsMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is an offset record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case offsetKey: OffsetKey =>
          val groupTopicPartition = offsetKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readOffsetMessageValue(ByteBuffer.wrap(value)).toString
          output.write(groupTopicPartition.toString.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

  // Formatter for use with tools to read group metadata history
  class GroupMetadataMessageFormatter extends MessageFormatter {
    def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream) {
      Option(consumerRecord.key).map(key => GroupMetadataManager.readMessageKey(ByteBuffer.wrap(key))).foreach {
        // Only print if the message is a group metadata record.
        // We ignore the timestamp of the message because GroupMetadataMessage has its own timestamp.
        case groupMetadataKey: GroupMetadataKey =>
          val groupId = groupMetadataKey.key
          val value = consumerRecord.value
          val formattedValue =
            if (value == null) "NULL"
            else GroupMetadataManager.readGroupMessageValue(groupId, ByteBuffer.wrap(value)).toString
          output.write(groupId.getBytes)
          output.write("::".getBytes)
          output.write(formattedValue.getBytes)
          output.write("\n".getBytes)
        case _ => // no-op
      }
    }
  }

}

case class DelayedStore(partitionRecords: Map[TopicPartition, MemoryRecords],
                        callback: Map[TopicPartition, PartitionResponse] => Unit)

case class GroupTopicPartition(group: String, topicPartition: TopicPartition) {

  def this(group: String, topic: String, partition: Int) =
    this(group, new TopicPartition(topic, partition))

  override def toString =
    "[%s,%s,%d]".format(group, topicPartition.topic, topicPartition.partition)
}

trait BaseKey{
  def version: Short
  def key: Object
}

case class OffsetKey(version: Short, key: GroupTopicPartition) extends BaseKey {

  override def toString = key.toString
}

case class GroupMetadataKey(version: Short, key: String) extends BaseKey {

  override def toString = key
}

