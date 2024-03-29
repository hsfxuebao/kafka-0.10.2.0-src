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
package kafka.controller

import collection._
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{StateChangeFailedException, TopicAndPartition}
import kafka.controller.Callbacks.CallbackBuilder
import kafka.utils.{Logging, ReplicationUtils, ZkUtils}
import kafka.utils.CoreUtils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
// 副本状态机，保存了集群所有的副本以及对应的副本状态
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkUtils = controllerContext.zkUtils
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val brokerChangeListener = new BrokerChangeListener(controller)
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
    // 启动副本状态机，初始化副本的状态
  def startup() {
    // initialize replica state
    initializeReplicaState()
    // set started flag
    hasStarted.set(true)
    // move all Online replicas to Online
      // 只针对存活的副本，触发 从 "上线"到"上线"的状态转换
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine
  def registerListeners() {
    // register broker change listener
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine
  def deregisterListeners() {
    // de-register broker change listener
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // reset replica state
    replicaState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
    // 副本状态机的状态改变处理方法（处理多个副本），以批量请求的方式发送给多个代理节点
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build) {
    if(replicas.nonEmpty) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        // todo 发送请求给其他所有的broker
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
    // 副本状态机（ReplicaStateMachine）针对单个副本的状态改变处理逻辑
    // 副本状态机处理副本状态转为"下线""开始删除"，都会发送StopReplica请求给代理节点
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks) {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica =>
          // 不存在状态  到 新建状态
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // start replica as a follower to the current leader for its partition
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkUtils, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            case Some(leaderIsrAndControllerEpoch) =>
              // 如果这个副本是主副本，不能转为新建，即上线状态不能逆转为新建状态
              if(leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              // 新创建的副本只能是备份副本，所以需要向新副本发送分区的主副本等其他信息
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                                                                  topic, partition, leaderIsrAndControllerEpoch,
                                                                  replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                            targetState))
          // 副本的前置状态必须是"下线"
        case ReplicaDeletionStarted =>
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          // send stop replica command
          // deletePartition = true 带有删除标记
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback) // 删除时，传递处理响应的回调方法
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionIneligible =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case ReplicaDeletionSuccessful =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case NonExistentReplica =>
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // remove this replica from the assigned replicas list for its partition
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        case OnlineReplica =>
          // 新建状态 在线状态 下线状态 删除失败状态 都可以到 上线状态
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          replicaState(partitionAndReplica) match {
            case NewReplica =>
              // add this replica to the assigned replicas list for its partition
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              // 如果不在上下文对象中，才添加到上下文对象中
              if(!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
              stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                                        .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
                                                targetState))
            case _ =>
              // check if the leader for this partition ever existed
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  replicaState.put(partitionAndReplica, OnlineReplica)
                  stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                    .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                case None => // that means the partition was never in OnlinePartition state, this means the broker never
                  // started a log for that partition and does not have a high watermark value for this partition
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
          // 副本的状态转为下线
        case OfflineReplica =>
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // send stop replica command to the replica so that it stops fetching from the leader
          // deletePartition = false 不带删除标记
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // As an optimization, the controller removes dead replicas from the ISR
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              case Some(_) =>
                // 将下线的副本从ISR集合中移除，更新zk节点中分区的ISR，"上下文对象"中分区的主副本
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    // 上一步中并未更新分区的副本信息
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                      // 向分区的副本集发送更新请求
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
                      .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
                    false
                  case None =>
                    true
                }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty && !controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition))
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
              .format(replicaId, topicAndPartition))
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    replicaStatesForTopic.forall(_._2 == ReplicaDeletionSuccessful)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set[ReplicaState](ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState) {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkUtils.zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    // 对/brokers/ids 目录设置了监听器
    zkUtils.zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
    // 初始化副本的状态，副本如果存活，状态为"上线"，如果不存活，副本为"删除失败"
  private def initializeReplicaState() {
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        if (controllerContext.liveBrokerIds.contains(replicaId))
          replicaState.put(partitionAndReplica, OnlineReplica)
        else
          // mark replicas on dead brokers as failed for topic deletion, if they belong to a topic to be deleted.
          // This is required during controller failover since during controller failover a broker can go down,
          // so the replicas on that broker should be moved to ReplicaDeletionIneligible to be on the safer side.
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  // 代理节点改变的监听器  监听 /brokers/ids 的子节点变化事件 定义在副本状态机中
  // 副本管理器注册"更改代理节点监听器"，在代理节点发生变化时，更新控制器的通道管理器
  class BrokerChangeListener(protected val controller: KafkaController) extends ControllerZkChildListener {

    protected def logName = "BrokerChangeListener"

    def doHandleChildChange(parentPath: String, currentBrokerList: Seq[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.sorted.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              // 获取到所有的broker
              val curBrokers = currentBrokerList.map(_.toInt).toSet.flatMap(zkUtils.getBrokerInfo)
              // 获取到所有broker 的id号 zk中最新的代理节点编号列表
              val curBrokerIds = curBrokers.map(_.id)
              // 获取到所有的 live 的broker
              val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
              // 新加入的broker = zk的代理节点列表 - 上下文对象的代理节点
              val newBrokerIds = curBrokerIds -- liveOrShuttingDownBrokerIds
              // 获取到宕机的broker = 上下文对象代理节点列表 - zk中代理节点列表
              val deadBrokerIds = liveOrShuttingDownBrokerIds -- curBrokerIds

              val newBrokers = curBrokers.filter(broker => newBrokerIds(broker.id))
              // 更新上下文对象的存储代理节点列表
              controllerContext.liveBrokers = curBrokers
              val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
              val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
              val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIdsSorted.mkString(","), deadBrokerIdsSorted.mkString(","), liveBrokerIdsSorted.mkString(",")))
              // 添加或删除代理节点，更新控制器通道管理器所管理的节点列表
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker)
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker)
              // 控制器处理代理节点的新增和删除事件
              if(newBrokerIds.nonEmpty)
                // newBrokerIds 是新注册上来的broker 新注册进来的broker 是如何处理的？
                controller.onBrokerStartup(newBrokerIdsSorted)
              // todo deadBrokerIds 是对宕机的broker进行处理
              if(deadBrokerIds.nonEmpty)
                controller.onBrokerFailure(deadBrokerIdsSorted)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }
