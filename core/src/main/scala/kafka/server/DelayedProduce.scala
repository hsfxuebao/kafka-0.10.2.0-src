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

import com.yammer.metrics.core.Meter
import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Pool
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse

import scala.collection._

case class ProducePartitionStatus(requiredOffset: Long, responseStatus: PartitionResponse) {
  @volatile var acksPending = false

  override def toString = "[acksPending: %b, error: %d, startOffset: %d, requiredOffset: %d]"
    .format(acksPending, responseStatus.error.code, responseStatus.baseOffset, requiredOffset)
}

/**
 * The produce metadata maintained by the delayed produce operation
 */
case class ProduceMetadata(produceRequiredAcks: Short,
                           produceStatus: Map[TopicPartition, ProducePartitionStatus]) {

  override def toString = "[requiredAcks: %d, partitionStatus: %s]"
    .format(produceRequiredAcks, produceStatus)
}

/**
 * A delayed produce operation that can be created by the replica manager and watched
 * in the produce operation purgatory
 * 延迟的生产
 */
class DelayedProduce(delayMs: Long,
                     produceMetadata: ProduceMetadata, // 生产请求相关的元数据
                     replicaManager: ReplicaManager,  // 副本管理器
                     responseCallback: Map[TopicPartition, PartitionResponse] => Unit)
  extends DelayedOperation(delayMs) {

  // first update the acks pending variable according to the error code
  // 初始化每个分区的应答状态，如果没有错误，acksPending表示应答正在进行中
  produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
    if (status.responseStatus.error == Errors.NONE) {
      // Timeout error state will be cleared when required acks are received
      status.acksPending = true
      status.responseStatus.error = Errors.REQUEST_TIMED_OUT
    } else {
      status.acksPending = false
    }

    trace("Initial partition status for %s is %s".format(topicPartition, status))
  }

  /**
   * The delayed produce operation can be completed if every partition
   * it produces to is satisfied by one of the following:
   *
   * Case A: This broker is no longer the leader: set an error in response
   * Case B: This broker is the leader:
   *   B.1 - If there was a local error thrown while checking if at least requiredAcks
   *         replicas have caught up to this operation: set an error in response
   *   B.2 - Otherwise, set the response with no error.
   */
    // 尝试完成延迟的生产
  override def tryComplete(): Boolean = {
    // check for each partition if it still has pending acks
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      trace(s"Checking produce satisfaction for ${topicPartition}, current status $status")
      // skip those partitions that have already been satisfied
      // 只检查acksPending=true的分区
      if (status.acksPending) {
        val (hasEnough, error) = replicaManager.getPartition(topicPartition) match {
            // 检查分区是否有副本
          case Some(partition) =>
            partition.checkEnoughReplicasReachOffset(status.requiredOffset)
          case None =>
            // Case A
            (false, Errors.UNKNOWN_TOPIC_OR_PARTITION)
        }
        // Case B.1 || B.2  有错误立即结束，没有错误并且有足够的副本结束
        // 只要有一个分区
        if (error != Errors.NONE || hasEnough) {
          status.acksPending = false
          status.responseStatus.error = error
        } // 其他情况（没有错误，但是副本不足）的分区的ackPending仍为true
      }
    }

    // check if every partition has satisfied at least one of case A or B
      // 当所有分区的ackPending的值为false时，尝试完成的结果为true
    if (!produceMetadata.produceStatus.values.exists(_.acksPending))
      forceComplete()
    else
      false
  }

  override def onExpiration() {
    produceMetadata.produceStatus.foreach { case (topicPartition, status) =>
      if (status.acksPending) {
        DelayedProduceMetrics.recordExpiration(topicPartition)
      }
    }
  }

  /**
   * Upon completion, return the current response status along with the error code per partition
   */
  override def onComplete() {
    // 延迟的生产完成时，返回生产响应结果给客户端
    val responseStatus = produceMetadata.produceStatus.mapValues(status => status.responseStatus)
    responseCallback(responseStatus)
  }
}

object DelayedProduceMetrics extends KafkaMetricsGroup {

  private val aggregateExpirationMeter = newMeter("ExpiresPerSec", "requests", TimeUnit.SECONDS)

  private val partitionExpirationMeterFactory = (key: TopicPartition) =>
    newMeter("ExpiresPerSec",
             "requests",
             TimeUnit.SECONDS,
             tags = Map("topic" -> key.topic, "partition" -> key.partition.toString))
  private val partitionExpirationMeters = new Pool[TopicPartition, Meter](valueFactory = Some(partitionExpirationMeterFactory))

  def recordExpiration(partition: TopicPartition) {
    aggregateExpirationMeter.mark()
    partitionExpirationMeters.getAndMaybePut(partition).mark()
  }
}

