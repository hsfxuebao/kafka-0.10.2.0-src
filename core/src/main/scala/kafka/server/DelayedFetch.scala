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

import java.util.concurrent.TimeUnit

import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotLeaderForPartitionException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.requests.FetchRequest.PartitionData

import scala.collection._

case class FetchPartitionStatus(startOffsetMetadata: LogOffsetMetadata, fetchInfo: PartitionData) {

  override def toString = "[startOffsetMetadata: " + startOffsetMetadata + ", " +
                          "fetchInfo: " + fetchInfo + "]"
}

/**
 * The fetch metadata maintained by the delayed fetch operation
 */
case class FetchMetadata(fetchMinBytes: Int,
                         fetchMaxBytes: Int,
                         hardMaxBytesLimit: Boolean,
                         fetchOnlyLeader: Boolean,
                         fetchOnlyCommitted: Boolean,
                         isFromFollower: Boolean,
                         replicaId: Int,
                         fetchPartitionStatus: Seq[(TopicPartition, FetchPartitionStatus)]) {

  override def toString = "[minBytes: " + fetchMinBytes + ", " +
                          "onlyLeader:" + fetchOnlyLeader + ", "
                          "onlyCommitted: " + fetchOnlyCommitted + ", "
                          "partitionStatus: " + fetchPartitionStatus + "]"
}
/**
 * A delayed fetch operation that can be created by the replica manager and watched
 * in the fetch operation purgatory
 * 延迟的拉取
 */
class DelayedFetch(delayMs: Long,
                   fetchMetadata: FetchMetadata,  // 拉取请求的相关元数据
                   replicaManager: ReplicaManager,  // 副本管理器
                   quota: ReplicaQuota,
                   responseCallback: Seq[(TopicPartition, FetchPartitionData)] => Unit)
  extends DelayedOperation(delayMs) {

  /**
   * The operation can be completed if:
   *
   * Case A: This broker is no longer the leader for some partitions it tries to fetch
   * Case B: This broker does not know of some partitions it tries to fetch
   * Case C: The fetch offset locates not on the last segment of the log
   * Case D: The accumulated bytes from all the fetching partitions exceeds the minimum bytes
   *
   * Upon completion, should return whatever data is available for each valid partition
   */
    // 尝试完成延迟的拉取
  override def tryComplete() : Boolean = {
    var accumulatedSize = 0
    var accumulatedThrottledSize = 0
    fetchMetadata.fetchPartitionStatus.foreach {
      case (topicPartition, fetchStatus) =>
        val fetchOffset = fetchStatus.startOffsetMetadata
        try {
          if (fetchOffset != LogOffsetMetadata.UnknownOffsetMetadata) {
            // 获取本地的主副本，因为延迟拉取在主副本所在的节点创建，所以一定能取到主副本
            val replica = replicaManager.getLeaderReplicaIfLocal(topicPartition)
            val endOffset =
              if (fetchMetadata.fetchOnlyCommitted)
                replica.highWatermark // 消费者的拉取，取主副本的最高水位
              else
                replica.logEndOffset  // 备份副本的拉取，取主副本的偏移量

            // Go directly to the check for Case D if the message offsets are the same. If the log segment
            // has just rolled, then the high watermark offset will remain the same but be on the old segment,
            // which would incorrectly be seen as an instance of Case C.
            if (endOffset.messageOffset != fetchOffset.messageOffset) {
              if (endOffset.onOlderSegment(fetchOffset)) {
                // Case C, this can happen when the new fetch operation is on a truncated leader
                debug("Satisfying fetch %s since it is fetching later segments of partition %s.".format(fetchMetadata, topicPartition))
                return forceComplete() // 拉取操作发生在被截断的主副本
              } else if (fetchOffset.onOlderSegment(endOffset)) {
                // Case C, this can happen when the fetch operation is falling behind the current segment
                // or the partition has just rolled a new segment
                debug("Satisfying fetch %s immediately since it is fetching older segments.".format(fetchMetadata))
                // We will not force complete the fetch request if a replica should be throttled.
                if (!replicaManager.shouldLeaderThrottle(quota, topicPartition, fetchMetadata.replicaId))
                  return forceComplete() // 拉取的偏移量和当前你偏移量所在的日志分段不同
              } else if (fetchOffset.messageOffset < endOffset.messageOffset) {
                // we take the partition fetch size as upper bound when accumulating the bytes (skip if a throttled partition)
                // 在同一个日志分段中
                val bytesAvailable = math.min(endOffset.positionDiff(fetchOffset), fetchStatus.fetchInfo.maxBytes)
                if (quota.isThrottled(topicPartition))
                  accumulatedThrottledSize += bytesAvailable
                else
                  accumulatedSize += bytesAvailable
              }
            } // 拉取偏移量和结束偏移量相等，说明读取到了主副本的最新位置了
          }
        } catch {
          case _: UnknownTopicOrPartitionException => // Case B
            debug("Broker no longer know of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
          case _: NotLeaderForPartitionException =>  // Case A
            debug("Broker is no longer the leader of %s, satisfy %s immediately".format(topicPartition, fetchMetadata))
            return forceComplete()
        }
    }

    // Case D
    if (accumulatedSize >= fetchMetadata.fetchMinBytes
      || ((accumulatedSize + accumulatedThrottledSize) >= fetchMetadata.fetchMinBytes && !quota.isQuotaExceeded()))
      forceComplete() // 收集到的所有消息超过fetchMinBytes，才会返回结果给客户端
    else
      false
  }

  override def onExpiration() {
    if (fetchMetadata.isFromFollower)
      DelayedFetchMetrics.followerExpiredRequestMeter.mark()
    else
      DelayedFetchMetrics.consumerExpiredRequestMeter.mark()
  }

  /**
   * Upon completion, read whatever data is available and pass to the complete callback
   * 延迟的拉取完成时，返回拉取响应结果给客户端
   */
  override def onComplete() {
    // 再根据拉取元数据，读取主副本的本地日志文件
    val logReadResults = replicaManager.readFromLocalLog(
      replicaId = fetchMetadata.replicaId,
      fetchOnlyFromLeader = fetchMetadata.fetchOnlyLeader,
      readOnlyCommitted = fetchMetadata.fetchOnlyCommitted,
      fetchMaxBytes = fetchMetadata.fetchMaxBytes,
      hardMaxBytesLimit = fetchMetadata.hardMaxBytesLimit,
      readPartitionInfo = fetchMetadata.fetchPartitionStatus.map { case (tp, status) => tp -> status.fetchInfo },
      quota = quota
    )
    // 返回拉取响应结果给客户端
    val fetchPartitionData = logReadResults.map { case (tp, result) =>
      tp -> FetchPartitionData(result.error, result.hw, result.info.records)
    }

    responseCallback(fetchPartitionData)
  }
}

object DelayedFetchMetrics extends KafkaMetricsGroup {
  private val FetcherTypeKey = "fetcherType"
  val followerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "follower"))
  val consumerExpiredRequestMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS, tags = Map(FetcherTypeKey -> "consumer"))
}

