/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupCoordinatorNotAvailableException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.GroupCoordinatorRequest;
import org.apache.kafka.common.requests.GroupCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.errors.InterruptException;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, Map)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 */

/**
 * 抽象的协调者管理了客户端的心跳状态和心跳定时任务
 */
public abstract class AbstractCoordinator implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(AbstractCoordinator.class);

    private enum MemberState {
        UNJOINED,    // the client is not part of a group
        REBALANCING, // the client has begun rebalancing
        STABLE,      // the client has joined and is sending heartbeats
    }

    protected final int rebalanceTimeoutMs;
    private final int sessionTimeoutMs;
    private final GroupCoordinatorMetrics sensors;
    // 心跳任务辅助类
    private final Heartbeat heartbeat;
    // 当前消费者所属的Consumer Group的id
    protected final String groupId;
    // 负责网络通信和执行定时任务
    protected final ConsumerNetworkClient client;
    protected final Time time;
    protected final long retryBackoffMs;

    // 心跳线程
    private HeartbeatThread heartbeatThread = null;
    // 是否重新发送JoinGroupRequest请求的条件之一
    private boolean rejoinNeeded = true;
    // 是否需要执行发送JoinGroupRequest请求前的准备操作
    private boolean needsJoinPrepare = true;
    private MemberState state = MemberState.UNJOINED;
    private RequestFuture<ByteBuffer> joinFuture = null;
    // 记录服务端GroupCoordinator所在的Node节点
    private Node coordinator = null;

    private Generation generation = Generation.NO_GENERATION;

    private RequestFuture<Void> findCoordinatorFuture = null;

    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs) {
        this.client = client;
        this.time = time;
        this.groupId = groupId;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.sessionTimeoutMs = sessionTimeoutMs;
        // 心跳操作辅助对象
        this.heartbeat = new Heartbeat(sessionTimeoutMs, heartbeatIntervalMs, rebalanceTimeoutMs, retryBackoffMs);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract List<ProtocolMetadata> metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 Map<String, ByteBuffer> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group.
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Block until the coordinator for this group is known and is ready to receive requests.
     */
    public synchronized void ensureCoordinatorReady() {
        // Using zero as current time since timeout is effectively infinite
        ensureCoordinatorReady(0, Long.MAX_VALUE);
    }

    /**
     * Ensure that the coordinator is ready to receive requests.
     * @param startTimeMs Current time in milliseconds
     * @param timeoutMs Maximum time to wait to discover the coordinator
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     *
     * 该方法会阻塞，直到GroupCoordinator已就绪且可以接受请求
     *    如果GroupCoordinator不正常，会发送GroupCoordinatorRequest请求
     */
    protected synchronized boolean ensureCoordinatorReady(long startTimeMs, long timeoutMs) {
        long remainingMs = timeoutMs;

        // 检测是否需要重新查找GroupCoordinator
        while (coordinatorUnknown()) {
            // todo
            RequestFuture<Void> future = lookupCoordinator();
            // 发送GroupCoordinatorRequest请求，该方法会阻塞，直到接收到GroupCoordinatorResponse响应
            client.poll(future, remainingMs);
            // 检测future的状态，查看是否有异常
            if (future.failed()) {
                // 出现异常
                if (future.isRetriable()) {
                    remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
                    if (remainingMs <= 0)
                        break;

                    log.debug("Coordinator discovery failed for group {}, refreshing metadata", groupId);
                    // 异常是RetriableException，则阻塞更新Metadata元数据
                    client.awaitMetadataUpdate(remainingMs);
                } else
                    // 异常不是RetriableException，抛出
                    throw future.exception();
            } else if (coordinator != null && client.connectionFailed(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                // 连接不到GroupCoordinator，退避一段时间后重试
                coordinatorDead();
                time.sleep(retryBackoffMs);
            }

            remainingMs = timeoutMs - (time.milliseconds() - startTimeMs);
            if (remainingMs <= 0)
                break;
        }

        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            // 任意找一台服务器
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                // TODO: If there are no brokers left, perhaps we should use the bootstrap set
                // from configuration?
                log.debug("No broker available to send GroupCoordinator request for group {}", groupId);
                // 无节点可用，返回包含NoAvailableBrokersException的RequestFuture
                return RequestFuture.noBrokersAvailable();
            } else
                // 需要查找GroupCoordinator
                // 查找负载最低的Node节点，创建GroupCoordinatorRequest请求
                // todo 发送请求
                findCoordinatorFuture = sendGroupCoordinatorRequest(node);
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes)
     * @return true if it should, false otherwise
     */
    protected synchronized boolean needRejoin() {
        return rejoinNeeded;
    }

    private synchronized boolean rejoinIncomplete() {
        return joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }

            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group, we don't need to send heartbeats
        if (state == MemberState.UNJOINED)
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        ensureCoordinatorReady();
        startHeartbeatThreadIfNeeded();
        // todo 发送注册请求的代码在这个里面
        joinGroupIfNeeded();
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private synchronized void disableHeartbeatThread() {
        if (heartbeatThread != null)
            heartbeatThread.disable();
    }

    // visible for testing. Joins the group without starting the heartbeat thread.
    void joinGroupIfNeeded() {
        while (needRejoin() || rejoinIncomplete()) {
            // 检测GroupCoordinator是否就绪
            ensureCoordinatorReady();

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            // 在Rebalance之前的准备工作，可能会提交Offset，调用使用者设置的Rebalance监听器
            if (needsJoinPrepare) {
                // 进行注册之前准备
                onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }

            // todo
            RequestFuture<ByteBuffer> future = initiateJoinGroup();
            // 使用ConsumerNetworkClient发送JoinGroupRequest，会阻塞直到收到JoinGroupResponse或出现异常
            client.poll(future);
            resetJoinGroupFuture();

            if (future.succeeded()) {
                needsJoinPrepare = true;
                //
                onJoinComplete(generation.generationId, generation.memberId, generation.protocol, future.value());
            } else {
                // 检测发送是否失败
                // 出现异常
                RuntimeException exception = future.exception();
                // 当异常是未知Consumer、正在重均衡、GroupCoordinator版本对不上时，直接尝试新的请求
                if (exception instanceof UnknownMemberIdException ||
                        exception instanceof RebalanceInProgressException ||
                        exception instanceof IllegalGenerationException)
                    continue;
                else if (!future.isRetriable())
                    // 不可重试，抛出异常
                    throw exception;
                // 可以重试，等待退避时间后再次重试
                time.sleep(retryBackoffMs);
            }
        }
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        if (joinFuture == null) {
            // fence off the heartbeat thread explicitly so that it cannot interfere with the join group.
            // Note that this must come after the call to onJoinPrepare since we must be able to continue
            // sending heartbeats if that callback takes some time.
            disableHeartbeatThread();

            state = MemberState.REBALANCING;
            // todo 发送一个注册的请求
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // handle join completion in the callback so that the callback will be invoked
                    // even if the consumer is woken up before finishing the rebalance
                    synchronized (AbstractCoordinator.this) {
                        log.info("Successfully joined group {} with generation {}", groupId, generation.generationId);
                        state = MemberState.STABLE;

                        if (heartbeatThread != null)
                            heartbeatThread.enable();
                    }
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin
                    synchronized (AbstractCoordinator.this) {
                        state = MemberState.UNJOINED;
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, Map)} if
     * elected leader by the coordinator.
     * @return A request future which wraps the assignment returned from the group leader
     */
    private RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        // 检查GroupCoordinator是否就绪
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group {}", groupId);
        // 构造JoinGroupRequest对象
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                groupId,
                this.sessionTimeoutMs,
                this.generation.memberId,
                protocolType(),
                metadata()).setRebalanceTimeout(this.rebalanceTimeoutMs);

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);
        // 使用ConsumerNetworkClient将请求暂存入unsent，等待发送  ApiKeys.JOIN_GROUP
        return client.send(coordinator, requestBuilder)
                // 适配响应处理器
                .compose(new JoinGroupResponseHandler());
    }

    // 处理JoinGroupResponse
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {

        // 处理JoinGroupResponse的流程入口
        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            // 获取错误并进行错误处理
            Errors error = Errors.forCode(joinResponse.errorCode());
            if (error == Errors.NONE) {
                // 无错误
                log.debug("Received successful JoinGroup response for group {}: {}", groupId, joinResponse);
                sensors.joinLatency.record(response.requestLatencyMs());

                synchronized (AbstractCoordinator.this) {
                    if (state != MemberState.REBALANCING) {
                        // if the consumer was woken up before a rebalance completes, we may have already left
                        // the group. In this case, we do not want to continue with the sync group.
                        future.raise(new UnjoinedGroupException());
                    } else {
                        AbstractCoordinator.this.generation = new Generation(joinResponse.generationId(),
                                joinResponse.memberId(), joinResponse.groupProtocol());
                        AbstractCoordinator.this.rejoinNeeded = false;
                        /**
                         * 消费组所有成员都会发送joinGroup 最终只有一个consumer是leader consumer
                         * 判断是否是Leader
                         */
                        if (joinResponse.isLeader()) {
                            // 如果是Leader，在该方法中会进行分区分配，并将分配结果反馈给coordinator
                            onJoinLeader(joinResponse).chain(future);
                        } else {
                            // 如果是Follower，也会发送进行同步分区分配的请求
                            onJoinFollower().chain(future);
                        }
                    }
                }
            } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                log.debug("Attempt to join group {} rejected since coordinator {} is loading the group.", groupId,
                        coordinator());
                // backoff and retry
                // 有异常，完成异步请求，但不成功
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // reset the member id and retry immediately
                resetGeneration();
                log.debug("Attempt to join group {} failed due to unknown member id.", groupId);
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // re-discover the coordinator and retry with backoff
                coordinatorDead();
                log.debug("Attempt to join group {} failed due to obsolete coordinator information: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID) {
                // log the error and re-throw the exception
                log.error("Attempt to join group {} failed due to fatal error: {}", groupId, error.message());
                future.raise(error);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                // unexpected error, throw the exception
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId,
                        Collections.<String, ByteBuffer>emptyMap());
        log.debug("Sending follower SyncGroup for group {} to coordinator {}: {}", groupId, this.coordinator,
                requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // todo 进行分区分配，最终返回的分配结果，其中键是ConsumerID，值是序列化后的Assignment对象
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.leaderId(), joinResponse.groupProtocol(),
                    joinResponse.members());
            // 将分组信息包装为SyncGroupRequest
            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(groupId, generation.generationId, generation.memberId, groupAssignment);
            log.debug("Sending leader SyncGroup for group {} to coordinator {}: {}",
                    groupId, this.coordinator, requestBuilder);
            // 发送分组信息，并返回
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        // 检查GroupCoordinator是否就绪
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        // 将发送分区分配信息的请求暂存到unsent集合， SYNC_GROUP
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler());
    }

    // 处理SyncGroupResponse响应的方法
    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        // 处理入口
        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = Errors.forCode(syncResponse.errorCode());
            if (error == Errors.NONE) {
                // 无异常
                sensors.syncLatency.record(response.requestLatencyMs());
                // 调用RequestFuture.complete()方法传播分区分配结果
                future.complete(syncResponse.memberAssignment());
            } else {
                // 有异常
                // 将rejoinNeeded置为true
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    // 组授权失败
                    future.raise(new GroupAuthorizationException(groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    // 正在Rebalance过程中
                    log.debug("SyncGroup for group {} failed due to coordinator rebalance", groupId);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    // 无效的Member ID，或无效的年代信息
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    resetGeneration();
                    future.raise(error);
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    // GroupCoordinator不可以用，没有Group对应的Coordinator
                    log.debug("SyncGroup for group {} failed due to {}", groupId, error);
                    coordinatorDead();
                    future.raise(error);
                } else {
                    // 其他异常
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendGroupCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending GroupCoordinator request for group {} to broker {}", groupId, node);
        // 创建GroupCoordinatorRequest请求
        GroupCoordinatorRequest.Builder requestBuilder =
                new GroupCoordinatorRequest.Builder(this.groupId);
        // 使用ConsumerNetworkClient发送请求，返回经过compose()适配的RequestFuture<Void>对象
        // todo 发送ApiKeys.GROUP_COORDINATOR 的请求
        return client.send(node, requestBuilder)
                    // 对响应进行处理
                     .compose(new GroupCoordinatorResponseHandler());
    }

    private class GroupCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received GroupCoordinator response {} for group {}", resp, groupId);

            // 将响应体封装为GroupCoordinatorResponse对象
            GroupCoordinatorResponse groupCoordinatorResponse = (GroupCoordinatorResponse) resp.responseBody();
            // use MAX_VALUE - node.id as the coordinator id to mimic separate connections
            // for the coordinator in the underlying network client layer
            // TODO: this needs to be better handled in KAFKA-1935
            // 获取响应中的异常信息
            Errors error = Errors.forCode(groupCoordinatorResponse.errorCode());
            clearFindCoordinatorFuture();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // 无异常，根据响应信息构建一个Node节点对象赋值给coordinator
                    AbstractCoordinator.this.coordinator = new Node(
                            // 从响应里面解析出coordinator服务器信息
                            Integer.MAX_VALUE - groupCoordinatorResponse.node().id(),
                            groupCoordinatorResponse.node().host(),
                            groupCoordinatorResponse.node().port());
                    log.info("Discovered coordinator {} for group {}.", coordinator, groupId);
                    // 尝试与coordinator建立连接
                    client.tryConnect(coordinator);
                    // 启动定时心跳任务
                    heartbeat.resetTimeouts(time.milliseconds());
                }
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                log.debug("Group coordinator lookup for group {} failed: {}", groupId, error.message());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        // coordinator是否为null
        return coordinator() == null;
    }

    /**
     * Get the current coordinator
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node coordinator() {
        if (coordinator != null && client.connectionFailed(coordinator)) {
            coordinatorDead();
            return null;
        }
        return this.coordinator;
    }

    /**
     * Mark the current coordinator as dead.
     */
    protected synchronized void coordinatorDead() {
        if (this.coordinator != null) {
            log.info("Marking the coordinator {} dead for group {}", this.coordinator, groupId);
            client.failUnsentRequests(this.coordinator, GroupCoordinatorNotAvailableException.INSTANCE);
            // 将GroupCoordinator设置为null，表示需要重选GroupCoordinator
            this.coordinator = null;
        }
    }

    /**
     * Get the current generation state if the group is stable.
     * @return the current generation or null if the group is unjoined/rebalancing
     */
    protected synchronized Generation generation() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    /**
     * Reset the generation and memberId because we have fallen out of the group.
     */
    protected synchronized void resetGeneration() {
        // 清空memberId
        this.generation = Generation.NO_GENERATION;
        this.rejoinNeeded = true;
        this.state = MemberState.UNJOINED;
    }

    protected synchronized void requestRejoin() {
        // 将rejoinNeeded置为true
        this.rejoinNeeded = true;
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public synchronized void close() {
        close(0);
    }

    protected synchronized void close(long timeoutMs) {
        if (heartbeatThread != null)
            heartbeatThread.close();
        maybeLeaveGroup();

        // At this point, there may be pending commits (async commits or sync commits that were
        // interrupted using wakeup) and the leave group request which have been queued, but not
        // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
        // If coordinator is not known, requests are aborted.
        Node coordinator = coordinator();
        if (coordinator != null && !client.awaitPendingRequests(coordinator, timeoutMs))
            log.warn("Close timed out with {} pending requests to coordinator, terminating client connections for group {}.",
                    client.pendingRequestCount(coordinator), groupId);
    }

    /**
     * Leave the current group and reset local generation/memberId.
     */
    public synchronized void maybeLeaveGroup() {
        if (!coordinatorUnknown() && state != MemberState.UNJOINED && generation != Generation.NO_GENERATION) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.debug("Sending LeaveGroup request to coordinator {} for group {}", coordinator, groupId);
            LeaveGroupRequest.Builder request =
                    new LeaveGroupRequest.Builder(groupId, generation.memberId);
            client.send(coordinator, request)
                    .compose(new LeaveGroupResponseHandler());
            client.pollNoWakeup();
        }

        resetGeneration();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            Errors error = Errors.forCode(leaveResponse.errorCode());
            if (error == Errors.NONE) {
                log.debug("LeaveGroup request for group {} returned successfully", groupId);
                future.complete(null);
            } else {
                log.debug("LeaveGroup request for group {} failed with error: {}", groupId, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request for group {} to coordinator {}", groupId, coordinator);
        // 创建心跳对象
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(this.groupId, this.generation.generationId, this.generation.memberId);
        // 使用ConsumerNetworkClient发送心跳，此时会将请求暂存到unsent集合，等待ConsumerNetworkClient的poll发送 HEARTBEAT
        return client.send(coordinator, requestBuilder)
                // 同时使用HeartbeatCompletionHandler将RequestFuture<ClientResponse>适配成RequestFuture<Void>
                .compose(new HeartbeatResponseHandler());
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {

        /**
         * 具体业务线由父类CoordinatorResponseHandler处理
         * 父类会调用该方法解析ClientResponse
         * 而HeartbeatCompletionHandler子类实现中才真正对ClientResponse进行解析
         * 最终解析为一个HeartbeatResponse对象
         * @param heartbeatResponse  已解析的响应
         * @param future
         */
        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatLatency.record(response.requestLatencyMs());
            // 获取响应错误
            Errors error = Errors.forCode(heartbeatResponse.errorCode());
            if (error == Errors.NONE) {
                // 没有响应错误
                log.debug("Received successful Heartbeat response for group {}", groupId);
                future.complete(null);
            } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                // GroupCoordinator不可用，或没有Group对应的Coordinator
                log.debug("Attempt to heartbeat failed for group {} since coordinator {} is either not started or not valid.",
                        groupId, coordinator());
                // 表示GroupCoordinator已失效
                coordinatorDead();
                // 抛出异常
                future.raise(error);
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // 正在Rebalance过程中
                log.debug("Attempt to heartbeat failed for group {} since it is rebalancing.", groupId);
                // 标记rejoinNeeded为true，重新发送JoinGroupRequest请求尝试重新加入Consumer Group
                requestRejoin();
                // 抛出异常
                future.raise(Errors.REBALANCE_IN_PROGRESS);
            } else if (error == Errors.ILLEGAL_GENERATION) {
                // 表示HeartbeatRequest携带的generationId过期，说明可能进行了一次Rebalance操作
                log.debug("Attempt to heartbeat failed for group {} since generation id is not legal.", groupId);
                // 表示HeartbeatRequest携带的generationId过期，说明可能进行了一次Rebalance操作
                resetGeneration();
                // 抛出异常
                future.raise(Errors.ILLEGAL_GENERATION);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                // GroupCoordinator无法识别该Consumer
                log.debug("Attempt to heartbeat failed for group {} since member id is not valid.", groupId);
                // 标记rejoinNeeded为true，重新发送JoinGroupRequest请求尝试重新加入Consumer Group
                resetGeneration();
                future.raise(Errors.UNKNOWN_MEMBER_ID);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // GroupCoordinator授权失败
                future.raise(new GroupAuthorizationException(groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        protected ClientResponse response;

        // 对解析后的响应进行处理
        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException)
                coordinatorDead();
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                // 解析响应
                R responseObj = (R) clientResponse.responseBody();
                // 处理响应
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatLatency;
        public final Sensor joinLatency;
        public final Sensor syncLatency;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatLatency = metrics.sensor("heartbeat-latency");
            this.heartbeatLatency.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatLatency.add(metrics.metricName("heartbeat-rate",
                this.metricGrpName,
                "The average number of heartbeats per second"), new Rate(new Count()));

            this.joinLatency = metrics.sensor("join-latency");
            this.joinLatency.add(metrics.metricName("join-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-time-max",
                    this.metricGrpName,
                    "The max time taken for a group rejoin"), new Avg());
            this.joinLatency.add(metrics.metricName("join-rate",
                    this.metricGrpName,
                    "The number of group joins per second"), new Rate(new Count()));

            this.syncLatency = metrics.sensor("sync-latency");
            this.syncLatency.add(metrics.metricName("sync-time-avg",
                    this.metricGrpName,
                    "The average time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-time-max",
                    this.metricGrpName,
                    "The max time taken for a group sync"), new Avg());
            this.syncLatency.add(metrics.metricName("sync-rate",
                    this.metricGrpName,
                    "The number of group syncs per second"), new Rate(new Count()));

            Measurable lastHeartbeat =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
                    }
                };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last controller heartbeat was sent"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread {
        private boolean enabled = false;
        private boolean closed = false;
        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super("kafka-coordinator-heartbeat-thread" + (groupId.isEmpty() ? "" : " | " + groupId), true);
        }

        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.trace("Enabling heartbeat thread for group {}", groupId);
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.trace("Disabling heartbeat thread for group {}", groupId);
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread for group {} started", groupId);
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        if (state != MemberState.STABLE) {
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        client.pollNoWakeup();
                        // 获取当前时间
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture == null)
                                lookupCoordinator();
                            else
                                AbstractCoordinator.this.wait(retryBackoffMs);
                            // 检测HeartbeatResponse是否超时，若超时则认为GroupCoordinator宕机
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            // 清空unsent集合中该GroupCoordinator所在Node对应的请求队列并将这些请求标记为异常
                            coordinatorDead();

                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            maybeLeaveGroup();

                        // 检测HeartbeatTask是否到期
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            /**
                             * 如果未到期，更新到期时间，将HeartbeatTask对象重新添加到DelayedTaskQueue中
                             * 注意，此时的时间已经更新为now + heartbeat.timeToNextHeartbeat(now)
                             */
                            AbstractCoordinator.this.wait(retryBackoffMs);
                        } else {
                            // 已到期，更新最近发送HeartbeatRequest请求的时间，即将lastHeartbeatSend更新为当前时间
                            heartbeat.sentHeartbeat(now);
                            // 在返回的RequestFuture上添加RequestFutureListener监听器 发送心跳的请求
                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();

                                            // wake up the thread if it's sleeping to reschedule the heartbeat

                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread for group {}", groupId, e);
                this.failed.set(new RuntimeException(e));
            } catch (RuntimeException e) {
                log.error("Heartbeat thread for group {} failed due to unexpected error" , groupId, e);
                this.failed.set(e);
            } finally {
                log.debug("Heartbeat thread for group {} has closed", groupId);
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        /**
         * 服务端GroupCoordinator返回的年代信息，用来区分两次Rebalance操作
         * 由于网络延迟等问题，在执行Rebalance操作时可能收到上次Rebalance过程的请求，
         * 为了避免这种干扰，每次Rebalance操作都会递增generation值
         */
        public final int generationId;
        // 服务端GroupCoordinator返回的分配给消费者的唯一ID
        public final String memberId;
        public final String protocol;

        public Generation(int generationId, String memberId, String protocol) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocol = protocol;
        }
    }

    private static class UnjoinedGroupException extends RetriableException {

    }

}
