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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.{OffsetAndMetadata, OffsetMetadataAndError, TopicAndPartition}
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{OffsetFetchResponse, JoinGroupRequest}

import scala.collection.{Map, Seq, immutable}


/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Short) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    if (enableMetadataExpiration)
      groupManager.enableMetadataExpiration()
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback) {
    // "加入组请求"前置条件判断，返回不同的错误，消费者根据不同的错误码都有不同的处理
    if (!isActive.get) {
      responseCallback(joinError(memberId, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!validGroupId(groupId)) {
      responseCallback(joinError(memberId, Errors.INVALID_GROUP_ID.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(joinError(memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(joinError(memberId, Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
               sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT.code))
    } else {
      // only try to create the group if the group is not unknown AND
      // the member id is UNKNOWN, if member is specified but group does not
      // exist we should reject the request
      groupManager.getGroup(groupId) match {
        case None =>
          if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID) {
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
          } else {
            val group = groupManager.addGroup(new GroupMetadata(groupId))
            // todo 核心代码
            doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          }

        case Some(group) =>
          doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
      }
    }
  }

  // 协调者处理消费者的"加入组请求"，将消费者添加到消费组的元数据中
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    // 同步块，针对消费组元数据操作时，不允许并发访问
    group synchronized {
      if (!group.is(Empty) && (group.protocolType != Some(protocolType) || !group.supportsProtocols(protocols.map(_._1).toSet))) {
        // if the new member does not support the group protocol, reject it
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL.code))
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))
      } else {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; this is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id,
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID.code))

          case PreparingRebalance =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // 消费者加入消费组中
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              // 更新消费组
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case AwaitingSync =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (memberId == group.leaderId) {
                    group.currentMemberMetadata
                  } else {
                    Map.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              } else {
                // member has changed metadata, so force a rebalance
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              }
            }

          case Empty | Stable =>
            if (memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID) {
              // if the member id is unknown, register the member to the group
              // todo 第一次进来
              addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, clientId, clientHost, protocolType, protocols, group, responseCallback)
            } else {
              val member = group.get(memberId)
              if (memberId == group.leaderId || !member.matches(protocols)) {
                // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
                // The latter allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                responseCallback(JoinGroupResult(
                  members = Map.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  subProtocol = group.protocol,
                  leaderId = group.leaderId,
                  errorCode = Errors.NONE.code))
              }
            }
        }

        if (group.is(PreparingRebalance))
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback) {
    if (!isActive.get) {
      responseCallback(Array.empty, Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Array.empty, Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else {
      groupManager.getGroup(groupId) match {
        case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
          // todo
        case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
      }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    var delayedGroupStore: Option[DelayedStore] = None

    group synchronized {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION.code)
      } else {
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID.code)

          case PreparingRebalance =>
            // 协调者处理消费者的同步组请求，如果消费组状态为准备再平衡，消费者应该重新加入消费组
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS.code)

            // 走到这
          case AwaitingSync =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 只有主消费者才会执行
            if (memberId == group.leaderId) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              // 第三个参数传递了一个高阶函数，类似于前面把"发送响应结果的回调方法"传给值对象
              delayedGroupStore = groupManager.prepareStoreGroup(group, assignment, (error: Errors) => {
                group synchronized {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the AwaitingSync state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(AwaitingSync) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group)
                    } else {
                      // todo coordinator 下发分区方案
                      setAndPropagateAssignment(group, assignment)
                      // 把状态切换成Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE.code)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }

    // store the group metadata without holding the group lock to avoid the potential
    // for deadlock if the callback is invoked holding other locks (e.g. the replica
    // state change lock)
    delayedGroupStore.foreach(groupManager.store)
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(Errors.GROUP_LOAD_IN_PROGRESS.code)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
          // joining without specified consumer id,
          responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

        case Some(group) =>
          group synchronized {
            if (group.is(Dead) || !group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
            } else {
              val member = group.get(memberId)
              removeHeartbeatForLeavingMember(group, member)
              onMemberFailure(group, member)
              responseCallback(Errors.NONE.code)
            }
          }
      }
    }
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Short => Unit) {
    if (!isActive.get) {
      responseCallback(Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code)
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(Errors.NOT_COORDINATOR_FOR_GROUP.code)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      // the group is still loading, so respond just blindly
      responseCallback(Errors.NONE.code)
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

        case Some(group) =>
          group synchronized {
            group.currentState match {
                // 消费组状态Dead
              case Dead =>
                // if the group is marked as dead, it means some other thread has just removed the group
                // from the coordinator metadata; this is likely that the group has migrated to some other
                // coordinator OR the group is in a transient unstable phase. Let the member retry
                // joining without the specified member id,
                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                //
              case Empty =>
                responseCallback(Errors.UNKNOWN_MEMBER_ID.code)

              case AwaitingSync =>
                // 不认识这个消费者
                if (!group.has(memberId))
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                else
                  responseCallback(Errors.REBALANCE_IN_PROGRESS.code)

              case PreparingRebalance =>
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION.code)
                } else {
                  val member = group.get(memberId)
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.REBALANCE_IN_PROGRESS.code)
                }

              case Stable =>
                // 必须稳定才能处理心跳
                if (!group.has(memberId)) {
                  responseCallback(Errors.UNKNOWN_MEMBER_ID.code)
                } else if (generationId != group.generationId) {
                  responseCallback(Errors.ILLEGAL_GENERATION.code)
                } else {
                  val member = group.get(memberId)
                  // todo
                  completeAndScheduleNextHeartbeatExpiration(group, member)
                  responseCallback(Errors.NONE.code) // 立即返回
                }
            }
          }
      }
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    if (!isActive.get) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_COORDINATOR_NOT_AVAILABLE.code))
    } else if (!isCoordinatorForGroup(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.NOT_COORDINATOR_FOR_GROUP.code))
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      responseCallback(offsetMetadata.mapValues(_ => Errors.GROUP_LOAD_IN_PROGRESS.code))
    } else {
      groupManager.getGroup(groupId) match {
        case None =>
          if (generationId < 0) {
            // the group is not relying on Kafka for group management, so allow the commit
            val group = groupManager.addGroup(new GroupMetadata(groupId))
            doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
          } else {
            // or this is a request coming from an older generation. either way, reject the commit
            responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
          }

        case Some(group) =>
          doCommitOffsets(group, memberId, generationId, offsetMetadata, responseCallback)
      }
    }
  }

  private def doCommitOffsets(group: GroupMetadata,
                      memberId: String,
                      generationId: Int,
                      offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                      responseCallback: immutable.Map[TopicPartition, Short] => Unit) {
    var delayedOffsetStore: Option[DelayedStore] = None

    group synchronized {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
      } else if (generationId < 0 && group.is(Empty)) {
        // the group is only using Kafka to store offsets
        delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
          offsetMetadata, responseCallback)
      } else if (group.is(AwaitingSync)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS.code))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID.code))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION.code))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        delayedOffsetStore = groupManager.prepareStoreOffsets(group, memberId, generationId,
          offsetMetadata, responseCallback)
      }
    }

    // store the offsets without holding the group lock
    delayedOffsetStore.foreach(groupManager.store)
  }

  def handleFetchOffsets(groupId: String,
                         partitions: Option[Seq[TopicPartition]] = None): (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {
    if (!isActive.get)
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, Map())
    else if (!isCoordinatorForGroup(groupId)) {
      debug("Could not fetch offsets for group %s (not group coordinator).".format(groupId))
      (Errors.NOT_COORDINATOR_FOR_GROUP, Map())
    } else if (isCoordinatorLoadingInProgress(groupId))
      (Errors.GROUP_LOAD_IN_PROGRESS, Map())
    else {
      // return offsets blindly regardless the current group state since the group may be using
      // Kafka commit storage without automatic group management
      (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading()) Errors.GROUP_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    if (!isActive.get) {
      (Errors.GROUP_COORDINATOR_NOT_AVAILABLE, GroupCoordinator.EmptyGroup)
    } else if (!isCoordinatorForGroup(groupId)) {
      (Errors.NOT_COORDINATOR_FOR_GROUP, GroupCoordinator.EmptyGroup)
    } else if (isCoordinatorLoadingInProgress(groupId)) {
      (Errors.GROUP_LOAD_IN_PROGRESS, GroupCoordinator.EmptyGroup)
    } else {
      groupManager.getGroup(groupId) match {
        case None => (Errors.NONE, GroupCoordinator.DeadGroup)
        case Some(group) =>
          group synchronized {
            (Errors.NONE, group.summary)
          }
      }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    groupManager.cleanupGroupMetadata(Some(topicPartitions))
  }

  // 消费组的协调者在删除元数据后，处理延迟请求相关的操作
  private def onGroupUnloaded(group: GroupMetadata) {
    group synchronized {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      // 消费组的状态转为Dead失败
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingJoinCallback != null) {
              member.awaitingJoinCallback(joinError(member.memberId, Errors.NOT_COORDINATOR_FOR_GROUP.code))
              member.awaitingJoinCallback = null
            }
          }
          // 所有消费者共用一个"延迟的加入"，都发送完"加入组响应"后执行
          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | AwaitingSync =>
          for (member <- group.allMemberMetadata) {
            // 返回SyncGroup响应
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR_FOR_GROUP.code)
              member.awaitingSyncCallback = null
            }
            // 每个消费者都有一个"延迟的心跳"，每个消费者发送完都执行
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  // 消费组的协调者在加载元数据后，处理延迟请求相关的操作
  private def onGroupLoaded(group: GroupMetadata) {
    group synchronized {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty)) // 确保消费组状态已经是"稳定"
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  // 消费组的协调者（GroupCoordinator）处理消费组的数据迁移
  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.loadGroupsForPartition(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    // todo
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(AwaitingSync))
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    // 遍历所有的member
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        // 调用回调函数 assignment 参数就是分配的分区消费方案
        // 其实coordinator 就是通过调用这个member回调函数  完成分区方案的下发
        member.awaitingSyncCallback(member.assignment, error.code) // 触发回调
        member.awaitingSyncCallback = null  // 重置回调方法

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def validGroupId(groupId: String): Boolean = {
    groupId != null && !groupId.isEmpty
  }

  private def joinError(memberId: String, errorCode: Short): JoinGroupResult = {
    JoinGroupResult(
      members=Map.empty,
      memberId=memberId,
      generationId=0,
      subProtocol=GroupCoordinator.NoProtocol,
      leaderId=GroupCoordinator.NoLeader,
      errorCode=errorCode)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    // complete current heartbeat expectation
    // 更新对应的consumer 的上一次心跳时间
    member.latestHeartbeat = time.milliseconds()
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    /**
     * 时间轮机制 往时间轮里面插入一个任务 这个任务就是用来检查心跳是否超时的
     * 当前时间11：00：00     设置一个时间轮任务  11：00：30开始延时任务
     * 去看最近30s 是否有心跳超时的
     */
    val newHeartbeatDeadline = member.latestHeartbeat + member.sessionTimeoutMs
    // 创建延迟的心跳
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member, newHeartbeatDeadline, member.sessionTimeoutMs)
    // 类似创建延迟的加入后，也会立即通过延迟缓存尝试完成，如果完成不了，则加入监控中
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
   * 添加成员，并执行再平衡
   */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,  // 重平衡超时时间
                                    sessionTimeoutMs: Int,  // 会话超时时间
                                    clientId: String, // 客户端编号
                                    clientHost: String, // 客户端地址
                                    protocolType: String, // 协议类型
                                    protocols: List[(String, Array[Byte])], // 消费者协议类型和元数据
                                    group: GroupMetadata, // 消费者组元数据
                                    callback: JoinCallback) = {
    // use the client-id with a random id suffix as the member-id
    // 协调者为发送"加入组请求"的消费者，在第一次创建成员元数据时指定成员编号
    val memberId = clientId + "-" + group.generateMemberIdSuffix
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)
    // 将回调方法设置为成员元数据 值对象的变量
    member.awaitingJoinCallback = callback  // 为值对象赋值
    // todo 添加自己的信息
    group.add(member)
    maybePrepareRebalance(group)  // 加入新成员，可能需要再平衡
    member
  }

  /**
   * 更新成员，并执行再平衡
   * @param group 消费者元数据
   * @param member  更新时已经存在消费者成员元数据
   * @param protocols 消费者的协议和元数据
   * @param callback  回调方法
   */
  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    member.supportedProtocols = protocols
    member.awaitingJoinCallback = callback
    // 更新成员，可能也需要再平衡
    maybePrepareRebalance(group)
  }

  private def maybePrepareRebalance(group: GroupMetadata) {
    // 同步块，可冲入锁，外层doJoinGroup方法调用
    group synchronized {
      // 状态为"等待同步"或"稳定时"
      if (group.canRebalance)
        prepareRebalance(group)
    }
  }

  private def prepareRebalance(group: GroupMetadata) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    // 如果有任何成员正在等待同步，取消它们的请求，让他们重新加入消费组
    if (group.is(AwaitingSync))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    // 状态改成"准备再平衡"
    group.transitionTo(PreparingRebalance)
    info("Preparing to restabilize group %s with old generation %s".format(group.groupId, group.generationId))

    // 再平衡操作的超时时间=max(消费组所有成员元数据会话超时时间)
    val rebalanceTimeout = group.rebalanceTimeoutMs
    // 创建延迟的加入组请求
    val delayedRebalance = new DelayedJoin(this, group, rebalanceTimeout)
    val groupKey = GroupKey(group.groupId)
    // 创建完延迟的操作对象后，立即尝试看能不能完成
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def onMemberFailure(group: GroupMetadata, member: MemberMetadata) {
    trace("Member %s in group %s has failed".format(member.memberId, group.groupId))
    // 将消费者移除消费组
    group.remove(member.memberId)
    group.currentState match {
      case Dead | Empty =>
      case Stable | AwaitingSync => maybePrepareRebalance(group)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  /**
   * 尝试完成延迟的加入操作，如果条件满足，能够完成，就调用forceComplete回调方法
   */
  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group synchronized {
      if (group.notYetRejoinedMembers.isEmpty)
        forceComplete()
      else false
    }
  }

  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata) {
    var delayedStore: Option[DelayedStore] = None
    group synchronized {
      // remove any members who haven't joined the group yet
      // todo group.notYetRejoinedMembers 找出没有重新发送"加入组请求"的消费者成员
      group.notYetRejoinedMembers.foreach { failedMember =>
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {
        // todo
        group.initNextGeneration()
        // 消费组中没有一个消费者
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty")

          delayedStore = groupManager.prepareStoreGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId}")

          // trigger the awaiting join group response callback for all the members after rebalancing
          // 获取消费组的所有消费者成员
          for (member <- group.allMemberMetadata) {
            // 必须确保成员元数据的回调方法不为空
            assert(member.awaitingJoinCallback != null)
            // 消费者元数据中解析"加入组响应结果"需要的数据，比如成员列表，成员编号等
            val joinResult = JoinGroupResult(
              members=if (member.memberId == group.leaderId) { group.currentMemberMetadata } else { Map.empty },
              memberId=member.memberId,
              generationId=group.generationId,  // 最新的纪元编号
              subProtocol=group.protocol, // 消费组协议
              leaderId=group.leaderId,  // 主消费者
              errorCode=Errors.NONE.code)

            // 调用消费者元数据的值对象，实际上调用了"发送响应的回调方法"
            member.awaitingJoinCallback(joinResult) // 触发回调
            member.awaitingJoinCallback = null  // 重置回调方法
            // 完成本次心跳，并调度下一次心跳
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }

    // call without holding the group lock
    delayedStore.foreach(groupManager.store)
  }

  /**
   * 尝试能否完成延迟的心跳
   */
  def tryCompleteHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long, forceComplete: () => Boolean) = {
    group synchronized {
      if (shouldKeepMemberAlive(member, heartbeatDeadline) || member.isLeaving)
        forceComplete()
      else false
    }
  }

  /**
   * 心跳超时
   */
  def onExpireHeartbeat(group: GroupMetadata, member: MemberMetadata, heartbeatDeadline: Long) {
    group synchronized {
      // 如果消费者成员仍然存活（多种可能的条件），则不会移除消费者
      if (!shouldKeepMemberAlive(member, heartbeatDeadline))
        // 消费者没有心跳了，将消费者从消费组中移除
        onMemberFailure(group, member)
    }
  }

  /**
   * 没有实质性的动作
   */
  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  // 消费者成员是否应该任务是存活的
  private def shouldKeepMemberAlive(member: MemberMetadata, heartbeatDeadline: Long) =
    member.awaitingJoinCallback != null ||  // 协调者正在处理消费者的加入组请求
      member.awaitingSyncCallback != null ||  // 协调者正在处理消费者的同步组请求
      // 消费者上一次心跳时间+会话超时时间 > 下一次心跳的截止时间，说明还是存活的
      member.latestHeartbeat + member.sessionTimeoutMs > heartbeatDeadline

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadingInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkUtils, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[coordinator] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkUtils: ZkUtils,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkUtils, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           errorCode: Short)
