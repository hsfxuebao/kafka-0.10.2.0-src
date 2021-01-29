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

import java.util.Properties

import kafka.admin.AdminUtils
import kafka.common.TopicAlreadyMarkedForDeletionException
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.common.errors.{ApiException, InvalidRequestException, PolicyViolationException}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.CreateTopicsRequest._
import org.apache.kafka.common.requests.CreateTopicsResponse
import org.apache.kafka.server.policy.CreateTopicPolicy
import org.apache.kafka.server.policy.CreateTopicPolicy.RequestMetadata

import scala.collection._
import scala.collection.JavaConverters._

class AdminManager(val config: KafkaConfig,
                   val metrics: Metrics,
                   val metadataCache: MetadataCache,
                   val zkUtils: ZkUtils) extends Logging with KafkaMetricsGroup {
  this.logIdent = "[Admin Manager on Broker " + config.brokerId + "]: "

  private val topicPurgatory = DelayedOperationPurgatory[DelayedOperation]("topic", config.brokerId)

  private val createTopicPolicy =
    Option(config.getConfiguredInstance(KafkaConfig.CreateTopicPolicyClassNameProp, classOf[CreateTopicPolicy]))

  def hasDelayedTopicOperations = topicPurgatory.delayed() != 0

  /**
    * Try to complete delayed topic operations with the request key
    */
  def tryCompleteDelayedTopicOperations(topic: String) {
    val key = TopicKey(topic)
    val completed = topicPurgatory.checkAndComplete(key)
    debug(s"Request key ${key.keyLabel} unblocked $completed topic requests.")
  }

  /**
    * Create topics and wait until the topics have been completely created.
    * The callback function will be triggered either when timeout, error or the topics are created.
    */
  def createTopics(timeout: Int,
                   validateOnly: Boolean,
                   createInfo: Map[String, TopicDetails],
                   responseCallback: Map[String, CreateTopicsResponse.Error] => Unit) {

    // 1. map over topics creating assignment and calling zookeeper
    val brokers = metadataCache.getAliveBrokers.map { b => kafka.admin.BrokerMetadata(b.id, b.rack) }
    val metadata = createInfo.map { case (topic, arguments) =>
      try {
        val configs = new Properties()
        arguments.configs.asScala.foreach { case (key, value) =>
          configs.setProperty(key, value)
        }
        LogConfig.validate(configs)

        val assignments = {
          if ((arguments.numPartitions != NO_NUM_PARTITIONS || arguments.replicationFactor != NO_REPLICATION_FACTOR)
            && !arguments.replicasAssignments.isEmpty)
            throw new InvalidRequestException("Both numPartitions or replicationFactor and replicasAssignments were set. " +
              "Both cannot be used at the same time.")
          else if (!arguments.replicasAssignments.isEmpty) {
            // Note: we don't check that replicaAssignment contains unknown brokers - unlike in add-partitions case,
            // this follows the existing logic in TopicCommand
            arguments.replicasAssignments.asScala.map { case (partitionId, replicas) =>
              (partitionId.intValue, replicas.asScala.map(_.intValue))
            }
          } else
            AdminUtils.assignReplicasToBrokers(brokers, arguments.numPartitions, arguments.replicationFactor)
        }
        trace(s"Assignments for topic $topic are $assignments ")

        createTopicPolicy match {
          case Some(policy) =>
            AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)

            // Use `null` for unset fields in the public API
            val numPartitions: java.lang.Integer =
              if (arguments.numPartitions == NO_NUM_PARTITIONS) null else arguments.numPartitions
            val replicationFactor: java.lang.Short =
              if (arguments.replicationFactor == NO_REPLICATION_FACTOR) null else arguments.replicationFactor
            val replicaAssignments = if (arguments.replicasAssignments.isEmpty) null else arguments.replicasAssignments

            policy.validate(new RequestMetadata(topic, numPartitions, replicationFactor, replicaAssignments,
              arguments.configs))

            if (!validateOnly)
              AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false)

          case None =>
            if (validateOnly)
              AdminUtils.validateCreateOrUpdateTopic(zkUtils, topic, assignments, configs, update = false)
            else
              AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, assignments, configs, update = false)
        }
        CreateTopicMetadata(topic, assignments, new CreateTopicsResponse.Error(Errors.NONE, null))
      } catch {
        // Log client errors at a lower level than unexpected exceptions
        case e@ (_: PolicyViolationException | _: ApiException) =>
          info(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreateTopicMetadata(topic, Map(), new CreateTopicsResponse.Error(Errors.forException(e), e.getMessage))
        case e: Throwable =>
          error(s"Error processing create topic request for topic $topic with arguments $arguments", e)
          CreateTopicMetadata(topic, Map(), new CreateTopicsResponse.Error(Errors.forException(e), e.getMessage))
      }
    }

    // 2. if timeout <= 0, validateOnly or no topics can proceed return immediately
    if (timeout <= 0 || validateOnly || !metadata.exists(_.error.is(Errors.NONE))) {
      val results = metadata.map { createTopicMetadata =>
        // ignore topics that already have errors
        if (createTopicMetadata.error.is(Errors.NONE) && !validateOnly) {
          (createTopicMetadata.topic, new CreateTopicsResponse.Error(Errors.REQUEST_TIMED_OUT, null))
        } else {
          (createTopicMetadata.topic, createTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the assignments and errors to the delayed operation and set the keys
      val delayedCreate = new DelayedCreateTopics(timeout, metadata.toSeq, this, responseCallback)
      val delayedCreateKeys = createInfo.keys.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedCreate, delayedCreateKeys)
    }
  }

  /**
    * Delete topics and wait until the topics have been completely deleted.
    * The callback function will be triggered either when timeout, error or the topics are deleted.
    */
  def deleteTopics(timeout: Int,
                   topics: Set[String],
                   responseCallback: Map[String, Errors] => Unit) {

    // 1. map over topics calling the asynchronous delete
    val metadata = topics.map { topic =>
        try {
          AdminUtils.deleteTopic(zkUtils, topic)
          DeleteTopicMetadata(topic, Errors.NONE)
        } catch {
          case _: TopicAlreadyMarkedForDeletionException =>
            // swallow the exception, and still track deletion allowing multiple calls to wait for deletion
            DeleteTopicMetadata(topic, Errors.NONE)
          case e: Throwable =>
            error(s"Error processing delete topic request for topic $topic", e)
            DeleteTopicMetadata(topic, Errors.forException(e))
        }
    }

    // 2. if timeout <= 0 or no topics can proceed return immediately
    if (timeout <= 0 || !metadata.exists(_.error == Errors.NONE)) {
      val results = metadata.map { deleteTopicMetadata =>
        // ignore topics that already have errors
        if (deleteTopicMetadata.error == Errors.NONE) {
          (deleteTopicMetadata.topic, Errors.REQUEST_TIMED_OUT)
        } else {
          (deleteTopicMetadata.topic, deleteTopicMetadata.error)
        }
      }.toMap
      responseCallback(results)
    } else {
      // 3. else pass the topics and errors to the delayed operation and set the keys
      val delayedDelete = new DelayedDeleteTopics(timeout, metadata.toSeq, this, responseCallback)
      val delayedDeleteKeys = topics.map(new TopicKey(_)).toSeq
      // try to complete the request immediately, otherwise put it into the purgatory
      topicPurgatory.tryCompleteElseWatch(delayedDelete, delayedDeleteKeys)
    }
  }

  def shutdown() {
    topicPurgatory.shutdown()
    CoreUtils.swallow(createTopicPolicy.foreach(_.close()))
  }
}
