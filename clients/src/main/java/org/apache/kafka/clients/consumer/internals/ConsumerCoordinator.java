/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.RetriableCommitFailedException;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.JoinGroupRequest.ProtocolMetadata;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.OffsetCommitResponse;
import org.apache.kafka.common.requests.OffsetFetchRequest;
import org.apache.kafka.common.requests.OffsetFetchResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manages the coordination process with the consumer coordinator.
 */
public final class ConsumerCoordinator extends AbstractCoordinator {

    private static final Logger log = LoggerFactory.getLogger(ConsumerCoordinator.class);

    /**
     * 在消费者发送的JoinGroupRequest请求中包含了消费者自身支持的PartitionAssignor信息，
     * GroupCoordinator从所有消费者都支持的分配策略中选择一个，通知Leader使用此分配策略进行分区分配。
     * 此字段的值通过partition.assignment.strategy参数配置，可以配置多个。
     */
    private final List<PartitionAssignor> assignors;
    // Kafka集群元数据
    private final Metadata metadata;
    private final ConsumerCoordinatorMetrics sensors;
    private final SubscriptionState subscriptions;
    private final OffsetCommitCallback defaultOffsetCommitCallback;
    // 是否开启了自动提交offset（enable.auto.commit）
    private final boolean autoCommitEnabled;
    // 每隔多久提交一次偏移量信息（消费偏移量）
    private final int autoCommitIntervalMs;
    // 拦截器集合
    private final ConsumerInterceptors<?, ?> interceptors;
    // 是否排除内部Topic
    private final boolean excludeInternalTopics;
    private final AtomicInteger pendingAsyncCommits;

    // this collection must be thread-safe because it is modified from the response handler
    // of offset commit requests, which may be invoked from the heartbeat thread
    private final ConcurrentLinkedQueue<OffsetCommitCompletion> completedOffsetCommits;

    private boolean isLeader = false;
    private Set<String> joinedSubscription;
    /**
     * 用来存储Metadata的快照信息，主要用来检测Topic是否发生了分区数量的变化。
     * 在ConsumerCoordinator的构造方法中，会为Metadata添加一个监听器，当Metadata更新时会做下面几件事：
     * - 如果是AUTO_PATTERN模式，则使用用户自定义的正则表达式过滤Topic，
     *      得到需要订阅的Topic集合后，设置到SubscriptionState的subscription集合和groupSubscription集合中。
     * - 如果是AUTO_PATTERN或AUTO_TOPICS模式，为当前Metadata做一个快照，这个快照底层是使用HashMap记录每个Topic中Partition的个数。
     *      将新旧快照进行比较，发生变化的话，则表示消费者订阅的Topic发生分区数量变化，
     *      则将SubscriptionState的needsPartitionAssignment字段置为true，需要重新进行分区分配。
     * - 使用metadataSnapshot字段记录变化后的新快照。
     */
    private MetadataSnapshot metadataSnapshot;
    /**
     * 用来存储Metadata的快照信息，不过是用来检测Partition分配的过程中有没有发生分区数量变化。
     * 具体是在Leader消费者开始分区分配操作前，使用此字段记录Metadata快照；
     * 收到SyncGroupResponse后，会比较此字段记录的快照与当前Metadata是否发生变化。
     * 如果发生变化，则要重新进行分区分配。在后面的介绍中还会分析上述过程。
     */
    private MetadataSnapshot assignmentSnapshot;
    private long nextAutoCommitDeadline;

    /**
     * Initialize the coordination manager.
     */
    public ConsumerCoordinator(ConsumerNetworkClient client,
                               String groupId,
                               int rebalanceTimeoutMs,
                               int sessionTimeoutMs,
                               int heartbeatIntervalMs,
                               List<PartitionAssignor> assignors,
                               Metadata metadata,
                               SubscriptionState subscriptions,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time,
                               long retryBackoffMs,
                               boolean autoCommitEnabled,
                               int autoCommitIntervalMs,
                               ConsumerInterceptors<?, ?> interceptors,
                               boolean excludeInternalTopics) {
        // 调用父类AbstractCoordinator的构造器
        super(client,
                groupId,
                rebalanceTimeoutMs,
                sessionTimeoutMs,
                heartbeatIntervalMs,
                metrics,
                metricGrpPrefix,
                time,
                retryBackoffMs);
        // 设置强制更新集群元数据
        this.metadata = metadata;
        // 根据消费者的SubscriptionState实例和集群元数据构建元数据快照
        this.metadataSnapshot = new MetadataSnapshot(subscriptions, metadata.fetch());
        // 记录消费者的SubscriptionState实例
        this.subscriptions = subscriptions;
        // offset提交回调
        this.defaultOffsetCommitCallback = new DefaultOffsetCommitCallback();
        // 是否自动提交offset
        this.autoCommitEnabled = autoCommitEnabled;

        this.autoCommitIntervalMs = autoCommitIntervalMs;
        // 分区分配器集合
        this.assignors = assignors;
        this.completedOffsetCommits = new ConcurrentLinkedQueue<>();
        this.sensors = new ConsumerCoordinatorMetrics(metrics, metricGrpPrefix);
        // 拦截器集合
        this.interceptors = interceptors;
        // 是否排除Kafka内部使用的Topic
        this.excludeInternalTopics = excludeInternalTopics;
        this.pendingAsyncCommits = new AtomicInteger();

        if (autoCommitEnabled)
            // 如果设置了自动提交offset，根据配置提交间隔时间
            this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;
        // 设置强制更新集群元数据
        this.metadata.requestUpdate();
        addMetadataListener();
    }

    @Override
    public String protocolType() {
        return ConsumerProtocol.PROTOCOL_TYPE;
    }

    /**
     * 消费者创建"加入组请求"需要指定消费者的元数据，比如订阅的主题
     */
    @Override
    public List<ProtocolMetadata> metadata() {
        this.joinedSubscription = subscriptions.subscription();
        List<ProtocolMetadata> metadataList = new ArrayList<>();
        for (PartitionAssignor assignor : assignors) {
            // 创建一个订阅状态，包含了消费者订阅的主题列表
            Subscription subscription = assignor.subscription(joinedSubscription);
            ByteBuffer metadata = ConsumerProtocol.serializeSubscription(subscription);
            // 每个分区分配器的元数据实际上都是一样的
            metadataList.add(new ProtocolMetadata(assignor.name(), metadata));
        }
        return metadataList;
    }

    public void updatePatternSubscription(Cluster cluster) {
        final Set<String> topicsToSubscribe = new HashSet<>();
        // 权限验证
        for (String topic : cluster.topics())
            if (subscriptions.subscribedPattern().matcher(topic).matches() &&
                    !(excludeInternalTopics && cluster.internalTopics().contains(topic)))
                topicsToSubscribe.add(topic);

        subscriptions.subscribeFromPattern(topicsToSubscribe);

        // note we still need to update the topics contained in the metadata. Although we have
        // specified that all topics should be fetched, only those set explicitly will be retained
        // 更新Metadata需要记录元数据的Topic集合
        metadata.setTopics(subscriptions.groupSubscription());
    }

    private void addMetadataListener() {
        this.metadata.addListener(new Metadata.Listener() {
            @Override
            public void onMetadataUpdate(Cluster cluster) {
                // if we encounter any unauthorized topics, raise an exception to the user
                // 当非AUTO_PATTERN模式时，如果非授权的主题不为空，则抛出异常
                if (!cluster.unauthorizedTopics().isEmpty())
                    throw new TopicAuthorizationException(new HashSet<>(cluster.unauthorizedTopics()));
                // AUTO_PATTERN模式的处理
                if (subscriptions.hasPatternSubscription())
                    updatePatternSubscription(cluster);

                // check if there are any changes to the metadata which should trigger a rebalance
                // 检测是否为AUTO_PATTERN或AUTO_TOPICS模式
                if (subscriptions.partitionsAutoAssigned()) {
                    // 根据新的subscriptions和cluster数据创建快照
                    MetadataSnapshot snapshot = new MetadataSnapshot(subscriptions, cluster);
                    // 比较新旧快照，如果不相等，则更新快照，并标识需要重新进行分区分配
                    if (!snapshot.equals(metadataSnapshot))
                        // 记录快照
                        metadataSnapshot = snapshot;
                }
            }
        });
    }

    private PartitionAssignor lookupAssignor(String name) {
        // 遍历所有的PartitionAssignor
        for (PartitionAssignor assignor : this.assignors) {
            // 匹配对应的PartitionAssignor：range或roundrobin
            if (assignor.name().equals(name))
                return assignor;
        }
        return null;
    }

    // 处理从SyncGroupResponse中的到的分区分配结果
    @Override
    protected void onJoinComplete(int generation,
                                  String memberId,
                                  String assignmentStrategy,
                                  ByteBuffer assignmentBuffer) {
        // only the leader is responsible for monitoring for metadata changes (i.e. partition changes)
        if (!isLeader)
            assignmentSnapshot = null;

        // 获取指定的分区器
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);
        // 获取分区分配信息
        Assignment assignment = ConsumerProtocol.deserializeAssignment(assignmentBuffer);

        // set the flag to refresh last committed offsets
        // 设置needsFetchCommittedOffsets为true，
        subscriptions.needRefreshCommits();

        // update partition assignment
        // 根据新的分区信息更新SubscriptionState的assignment集合
        subscriptions.assignFromSubscribed(assignment.partitions());

        // check if the assignment contains some topics that were not in the original
        // subscription, if yes we will obey what leader has decided and add these topics
        // into the subscriptions as long as they still match the subscribed pattern
        //
        // TODO this part of the logic should be removed once we allow regex on leader assign
        Set<String> addedTopics = new HashSet<>();
        for (TopicPartition tp : subscriptions.assignedPartitions()) {
            if (!joinedSubscription.contains(tp.topic()))
                addedTopics.add(tp.topic());
        }

        if (!addedTopics.isEmpty()) {
            Set<String> newSubscription = new HashSet<>(subscriptions.subscription());
            Set<String> newJoinedSubscription = new HashSet<>(joinedSubscription);
            newSubscription.addAll(addedTopics);
            newJoinedSubscription.addAll(addedTopics);

            this.subscriptions.subscribeFromPattern(newSubscription);
            this.joinedSubscription = newJoinedSubscription;
        }

        // update the metadata and enforce a refresh to make sure the fetcher can start
        // fetching data in the next iteration
        this.metadata.setTopics(subscriptions.groupSubscription());
        client.ensureFreshMetadata();

        // give the assignor a chance to update internal state based on the received assignment
        // 将assignment传递给onAssignment()方法，让分区分配器有机会更新内部状态
        // 该方法默认是空实现，用户自定义分区器时可以重写该方法
        assignor.onAssignment(assignment);

        // reschedule the auto commit starting from now
        this.nextAutoCommitDeadline = time.milliseconds() + autoCommitIntervalMs;

        // execute the user's callback after rebalance
        // 获取Rebalance监听器
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Setting newly assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> assigned = new HashSet<>(subscriptions.assignedPartitions());
            // 调用监听器
            listener.onPartitionsAssigned(assigned);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition assignment",
                    listener.getClass().getName(), groupId, e);
        }
    }

    /**
     * Poll for coordinator events. This ensures that the coordinator is known and that the consumer
     * has joined the group (if it is using group management). This also handles periodic offset commits
     * if they are enabled.
     *
     * @param now current time in milliseconds
     */
    public void poll(long now) {
        invokeCompletedOffsetCommitCallbacks();

        if (subscriptions.partitionsAutoAssigned() && coordinatorUnknown()) {
            // todo 确保GroupCoordinator已就绪，如果没有就绪会一直阻塞
            // 计算出来那台服务器是coordinator服务器
            ensureCoordinatorReady();
            now = time.milliseconds();
        }

        if (needRejoin()) {
            // due to a race condition between the initial metadata fetch and the initial rebalance,
            // we need to ensure that the metadata is fresh before joining initially. This ensures
            // that we have matched the pattern against the cluster's topics at least once before joining.
            if (subscriptions.hasPatternSubscription())
                client.ensureFreshMetadata();

            // todo 核心代码
            ensureActiveGroup();
            now = time.milliseconds();
        }

        pollHeartbeat(now);
        maybeAutoCommitOffsetsAsync(now);
    }

    /**
     * Return the time to the next needed invocation of {@link #poll(long)}.
     * @param now current time in milliseconds
     * @return the maximum time in milliseconds the caller should wait before the next invocation of poll()
     */
    public long timeToNextPoll(long now) {
        if (!autoCommitEnabled)
            return timeToNextHeartbeat(now);

        if (now > nextAutoCommitDeadline)
            return 0;

        return Math.min(nextAutoCommitDeadline - now, timeToNextHeartbeat(now));
    }

    /**
     * 主消费者执行分区分配，返回每个消费者的分区分配结果
     */
    @Override
    protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                        String assignmentStrategy,
                                                        Map<String, ByteBuffer> allSubscriptions) {

        // 根据协调者指定的消费组协议，获取唯一的分区分配器
        PartitionAssignor assignor = lookupAssignor(assignmentStrategy);
        if (assignor == null)
            throw new IllegalStateException("Coordinator selected invalid assignment protocol: " + assignmentStrategy);

        Set<String> allSubscribedTopics = new HashSet<>();
        // subscriptions 是从所有消费者的订阅元数据中解析出来的
        Map<String, Subscription> subscriptions = new HashMap<>();
        for (Map.Entry<String, ByteBuffer> subscriptionEntry : allSubscriptions.entrySet()) {
            // 反序列化消费者的订阅消息
            Subscription subscription = ConsumerProtocol.deserializeSubscription(subscriptionEntry.getValue());
            // 键：消费者成员编号 值：订阅的主题
            subscriptions.put(subscriptionEntry.getKey(), subscription);
            // 所有消费者订阅的所有主题，集群元数据会获取这些主题的所有分区
            allSubscribedTopics.addAll(subscription.topics());
        }

        // the leader will begin watching for changes to any of the topics the group is interested in,
        // which ensures that all metadata changes will eventually be seen
        this.subscriptions.groupSubscribe(allSubscribedTopics);
        metadata.setTopics(this.subscriptions.groupSubscription());

        // update metadata (if needed) and keep track of the metadata used for assignment so that
        // we can check after rebalance completion whether anything has changed
        client.ensureFreshMetadata();

        isLeader = true;

        log.debug("Performing assignment for group {} using strategy {} with subscriptions {}",
                groupId, assignor.name(), subscriptions);

        // 根据分配策略，为所有消费者分配分区，返回值表示每个消费者的分配结果
        Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), subscriptions);

        // user-customized assignor may have created some topics that are not in the subscription list
        // and assign their partitions to the members; in this case we would like to update the leader's
        // own metadata with the newly added topics so that it will not trigger a subsequent rebalance
        // when these topics gets updated from metadata refresh.
        //
        // TODO: this is a hack and not something we want to support long-term unless we push regex into the protocol
        //       we may need to modify the PartitionAssingor API to better support this case.
        Set<String> assignedTopics = new HashSet<>();
        for (Assignment assigned : assignment.values()) {
            //
            for (TopicPartition tp : assigned.partitions())
                assignedTopics.add(tp.topic());
        }

        if (!assignedTopics.containsAll(allSubscribedTopics)) {
            Set<String> notAssignedTopics = new HashSet<>(allSubscribedTopics);
            notAssignedTopics.removeAll(assignedTopics);
            log.warn("The following subscribed topics are not assigned to any members in the group {} : {} ", groupId,
                    notAssignedTopics);
        }

        if (!allSubscribedTopics.containsAll(assignedTopics)) {
            Set<String> newlyAddedTopics = new HashSet<>(assignedTopics);
            newlyAddedTopics.removeAll(allSubscribedTopics);
            log.info("The following not-subscribed topics are assigned to group {}, and their metadata will be " +
                    "fetched from the brokers : {}", groupId, newlyAddedTopics);

            allSubscribedTopics.addAll(assignedTopics);
            this.subscriptions.groupSubscribe(allSubscribedTopics);
            metadata.setTopics(this.subscriptions.groupSubscription());
            client.ensureFreshMetadata();
        }

        assignmentSnapshot = metadataSnapshot;

        log.debug("Finished assignment for group {}: {}", groupId, assignment);
        // 由于分区分配器返回结果的值是一个Assignment对象，要转换成字节数据返回给协调者
        Map<String, ByteBuffer> groupAssignment = new HashMap<>();
        for (Map.Entry<String, Assignment> assignmentEntry : assignment.entrySet()) {
            // 序列化存储
            ByteBuffer buffer = ConsumerProtocol.serializeAssignment(assignmentEntry.getValue());
            groupAssignment.put(assignmentEntry.getKey(), buffer);
        }

        return groupAssignment;
    }

    // 消费者在发送"加入组请求"之前，要先清除当前的分配结果
    @Override
    protected void onJoinPrepare(int generation, String memberId) {
        // commit offsets prior to rebalance if auto-commit enabled
        // 保存当前分区的消费进度，如果开启自动提交任务，会暂停掉
        maybeAutoCommitOffsetsSync(rebalanceTimeoutMs);

        // execute the user's callback before rebalance
        // 加入组之前，消费者已经有分区，这些分区将被清除，触发自定义监听器的回调
        ConsumerRebalanceListener listener = subscriptions.listener();
        log.info("Revoking previously assigned partitions {} for group {}", subscriptions.assignedPartitions(), groupId);
        try {
            Set<TopicPartition> revoked = new HashSet<>(subscriptions.assignedPartitions());
            listener.onPartitionsRevoked(revoked);
        } catch (WakeupException | InterruptException e) {
            throw e;
        } catch (Exception e) {
            log.error("User provided listener {} for group {} failed on partition revocation",
                    listener.getClass().getName(), groupId, e);
        }

        isLeader = false;
        // 重置组订阅信息
        subscriptions.resetGroupSubscription();
    }

    @Override
    public boolean needRejoin() {
        if (!subscriptions.partitionsAutoAssigned())
            return false;

        // we need to rejoin if we performed the assignment and metadata has changed
        if (assignmentSnapshot != null && !assignmentSnapshot.equals(metadataSnapshot))
            return true;

        // we need to join if our subscription has changed since the last join
        if (joinedSubscription != null && !joinedSubscription.equals(subscriptions.subscription()))
            return true;

        return super.needRejoin();
    }

    /**
     * Refresh the committed offsets for provided partitions.
     */
    public void refreshCommittedOffsetsIfNeeded() {
        if (subscriptions.refreshCommitsNeeded()) {
            Map<TopicPartition, OffsetAndMetadata> offsets = fetchCommittedOffsets(subscriptions.assignedPartitions());
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                // verify assignment is still active
                if (subscriptions.isAssigned(tp))
                    // 更新分区状态的committed 变量 协调节点保存的数据更新到客户端
                    this.subscriptions.committed(tp, entry.getValue());
            }
            this.subscriptions.commitsRefreshed();
        }
    }

    /**
     * Fetch the current committed offsets from the coordinator for a set of partitions.
     * @param partitions The partitions to fetch offsets for
     * @return A map from partition to the committed offset
     */
    public Map<TopicPartition, OffsetAndMetadata> fetchCommittedOffsets(Set<TopicPartition> partitions) {
        while (true) {
            ensureCoordinatorReady();

            // contact coordinator to fetch committed offsets
            // 发送OFFSET_FETCH 请求给协调者  获取分区已经提交的偏移量
            RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future = sendOffsetFetchRequest(partitions);
            client.poll(future);

            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);
        }
    }

    public void close(long timeoutMs) {
        // we do not need to re-enable wakeups since we are closing already
        client.disableWakeups();

        long now = time.milliseconds();
        long endTimeMs = now + timeoutMs;
        try {
            maybeAutoCommitOffsetsSync(timeoutMs);
            now = time.milliseconds();
            if (pendingAsyncCommits.get() > 0 && endTimeMs > now) {
                ensureCoordinatorReady(now, endTimeMs - now);
                now = time.milliseconds();
            }
        } finally {
            super.close(Math.max(0, endTimeMs - now));
        }
    }

    // visible for testing
    void invokeCompletedOffsetCommitCallbacks() {
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null)
                break;
            completion.invoke();
        }
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        /** 其实提交偏移量信息就是提交coordinator
         *
         *  offset -> __conusumer_offset 默认有50个分区 -> 4  leader partition 在哪个主机 这个主机就是coordinator
         *  同时，这个消费组偏移量信息也是提交到这个一台服务器（partition 这个leader partition）
         */
        if (!coordinatorUnknown()) {
            // todo
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets, new RetriableCommitFailedException(e)));
                }
            });
        }

        // ensure the commit has a chance to be transmitted (without blocking on its completion).
        // Note that commits are treated as heartbeats by the coordinator, so there is no need to
        // explicitly allow heartbeats through delayed task execution.
        client.pollNoWakeup();
    }

    private void doCommitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        this.subscriptions.needRefreshCommits();
        // 发送提交偏移量的请求
        RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
        final OffsetCommitCallback cb = callback == null ? defaultOffsetCommitCallback : callback;
        future.addListener(new RequestFutureListener<Void>() {
            @Override
            public void onSuccess(Void value) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, null));
            }

            @Override
            public void onFailure(RuntimeException e) {
                Exception commitException = e;

                if (e instanceof RetriableException)
                    commitException = new RetriableCommitFailedException(e);

                completedOffsetCommits.add(new OffsetCommitCompletion(cb, offsets, commitException));
            }
        });
    }

    /**
     * Commit offsets synchronously. This method will retry until the commit completes successfully
     * or an unrecoverable error is encountered.
     * @param offsets The offsets to be committed
     * @throws org.apache.kafka.common.errors.AuthorizationException if the consumer is not authorized to the group
     *             or to any of the specified partitions
     * @throws CommitFailedException if an unrecoverable error occurs before the commit can be completed
     * @return If the offset commit was successfully sent and a successful response was received from
     *         the coordinator
     */
    public boolean commitOffsetsSync(Map<TopicPartition, OffsetAndMetadata> offsets, long timeoutMs) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty())
            return true;

        long now = time.milliseconds();
        long startMs = now;
        long remainingMs = timeoutMs;
        do {
            if (coordinatorUnknown()) {
                if (!ensureCoordinatorReady(now, remainingMs))
                    return false;

                remainingMs = timeoutMs - (time.milliseconds() - startMs);
            }

            RequestFuture<Void> future = sendOffsetCommitRequest(offsets);
            client.poll(future, remainingMs);

            if (future.succeeded()) {
                if (interceptors != null)
                    interceptors.onCommit(offsets);
                return true;
            }

            if (!future.isRetriable())
                throw future.exception();

            time.sleep(retryBackoffMs);

            now = time.milliseconds();
            remainingMs = timeoutMs - (now - startMs);
        } while (remainingMs > 0);

        return false;
    }

    private void maybeAutoCommitOffsetsAsync(long now) {
        if (autoCommitEnabled) {
            if (coordinatorUnknown()) {
                this.nextAutoCommitDeadline = now + retryBackoffMs;
            } else if (now >= nextAutoCommitDeadline) {
                this.nextAutoCommitDeadline = now + autoCommitIntervalMs;
                // todo 提交偏移量
                doAutoCommitOffsetsAsync();
            }
        }
    }

    public void maybeAutoCommitOffsetsNow() {
        if (autoCommitEnabled && !coordinatorUnknown())
            doAutoCommitOffsetsAsync();
    }

    private void doAutoCommitOffsetsAsync() {
        Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
        log.debug("Sending asynchronous auto-commit of offsets {} for group {}", allConsumedOffsets, groupId);

        commitOffsetsAsync(allConsumedOffsets, new OffsetCommitCallback() {
            @Override
            public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                    log.warn("Auto-commit of offsets {} failed for group {}: {}", offsets, groupId,
                            exception.getMessage());
                    if (exception instanceof RetriableException)
                        nextAutoCommitDeadline = Math.min(time.milliseconds() + retryBackoffMs, nextAutoCommitDeadline);
                } else {
                    log.debug("Completed auto-commit of offsets {} for group {}", offsets, groupId);
                }
            }
        });
    }

    private void maybeAutoCommitOffsetsSync(long timeoutMs) {
        if (autoCommitEnabled) {
            Map<TopicPartition, OffsetAndMetadata> allConsumedOffsets = subscriptions.allConsumed();
            try {
                log.debug("Sending synchronous auto-commit of offsets {} for group {}", allConsumedOffsets, groupId);
                if (!commitOffsetsSync(allConsumedOffsets, timeoutMs))
                    log.debug("Auto-commit of offsets {} for group {} timed out before completion",
                            allConsumedOffsets, groupId);
            } catch (WakeupException | InterruptException e) {
                log.debug("Auto-commit of offsets {} for group {} was interrupted before completion",
                        allConsumedOffsets, groupId);
                // rethrow wakeups since they are triggered by the user
                throw e;
            } catch (Exception e) {
                // consistent with async auto-commit failures, we do not propagate the exception
                log.warn("Auto-commit of offsets {} failed for group {}: {}", allConsumedOffsets, groupId,
                        e.getMessage());
            }
        }
    }

    private class DefaultOffsetCommitCallback implements OffsetCommitCallback {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            if (exception != null)
                log.error("Offset commit with offsets {} failed for group {}", offsets, groupId, exception);
        }
    }

    /**
     * Commit offsets for the specified list of topics and partitions. This is a non-blocking call
     * which returns a request future that can be polled in the case of a synchronous commit or ignored in the
     * asynchronous case.
     *
     * @param offsets The list of offsets per partition that should be committed.
     * @return A request future whose value indicates whether the commit was successful or not
     */
    private RequestFuture<Void> sendOffsetCommitRequest(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets.isEmpty())
            return RequestFuture.voidSuccess();

        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        // create the offset commit request
        Map<TopicPartition, OffsetCommitRequest.PartitionData> offsetData = new HashMap<>(offsets.size());
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            if (offsetAndMetadata.offset() < 0) {
                return RequestFuture.failure(new IllegalArgumentException("Invalid offset: " + offsetAndMetadata.offset()));
            }
            offsetData.put(entry.getKey(), new OffsetCommitRequest.PartitionData(
                    offsetAndMetadata.offset(), offsetAndMetadata.metadata()));
        }

        final Generation generation;
        if (subscriptions.partitionsAutoAssigned())
            generation = generation();
        else
            generation = Generation.NO_GENERATION;

        // if the generation is null, we are not part of an active group (and we expect to be).
        // the only thing we can do is fail the commit and let the user rejoin the group in poll()
        if (generation == null)
            return RequestFuture.failure(new CommitFailedException());

        // 封装请求
        OffsetCommitRequest.Builder builder =
                new OffsetCommitRequest.Builder(this.groupId, offsetData).
                        setGenerationId(generation.generationId).
                        setMemberId(generation.memberId).
                        setRetentionTime(OffsetCommitRequest.DEFAULT_RETENTION_TIME);

        log.trace("Sending OffsetCommit request with {} to coordinator {} for group {}", offsets, coordinator, groupId);

        // 发送请求 OFFSET_COMMIT
        return client.send(coordinator, builder)
                .compose(new OffsetCommitResponseHandler(offsets));
    }

    private class OffsetCommitResponseHandler extends CoordinatorResponseHandler<OffsetCommitResponse, Void> {

        private final Map<TopicPartition, OffsetAndMetadata> offsets;

        private OffsetCommitResponseHandler(Map<TopicPartition, OffsetAndMetadata> offsets) {
            this.offsets = offsets;
        }

        @Override
        public void handle(OffsetCommitResponse commitResponse, RequestFuture<Void> future) {
            sensors.commitLatency.record(response.requestLatencyMs());
            Set<String> unauthorizedTopics = new HashSet<>();

            for (Map.Entry<TopicPartition, Short> entry : commitResponse.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetAndMetadata offsetAndMetadata = this.offsets.get(tp);
                long offset = offsetAndMetadata.offset();

                Errors error = Errors.forCode(entry.getValue());
                if (error == Errors.NONE) {
                    log.debug("Group {} committed offset {} for partition {}", groupId, offset, tp);
                    if (subscriptions.isAssigned(tp))
                        // update the local cache only if the partition is still assigned
                        subscriptions.committed(tp, offsetAndMetadata);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    log.error("Not authorized to commit offsets for group {}", groupId);
                    future.raise(new GroupAuthorizationException(groupId));
                    return;
                } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                    unauthorizedTopics.add(tp.topic());
                } else if (error == Errors.OFFSET_METADATA_TOO_LARGE
                        || error == Errors.INVALID_COMMIT_OFFSET_SIZE) {
                    // raise the error to the user
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    future.raise(error);
                    return;
                } else if (error == Errors.GROUP_COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR_FOR_GROUP
                        || error == Errors.REQUEST_TIMED_OUT) {
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    coordinatorDead();
                    future.raise(error);
                    return;
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION
                        || error == Errors.REBALANCE_IN_PROGRESS) {
                    // need to re-join group
                    log.debug("Offset commit for group {} failed: {}", groupId, error.message());
                    resetGeneration();
                    future.raise(new CommitFailedException());
                    return;
                } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                    log.debug("Offset commit for group {} failed on partition {}: {}", groupId, tp, error.message());
                    future.raise(new KafkaException("Partition " + tp + " may not exist or user may not have Describe access to topic"));
                    return;
                } else {
                    log.error("Group {} failed to commit partition {} at offset {}: {}", groupId, tp, offset, error.message());
                    future.raise(new KafkaException("Unexpected error in commit: " + error.message()));
                    return;
                }
            }

            if (!unauthorizedTopics.isEmpty()) {
                log.error("Not authorized to commit to topics {} for group {}", unauthorizedTopics, groupId);
                future.raise(new TopicAuthorizationException(unauthorizedTopics));
            } else {
                future.complete(null);
            }
        }
    }

    /**
     * Fetch the committed offsets for a set of partitions. This is a non-blocking call. The
     * returned future can be polled to get the actual offsets returned from the broker.
     *
     * @param partitions The set of partitions to get offsets for.
     * @return A request future containing the committed offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetAndMetadata>> sendOffsetFetchRequest(Set<TopicPartition> partitions) {
        Node coordinator = coordinator();
        if (coordinator == null)
            return RequestFuture.coordinatorNotAvailable();

        log.debug("Group {} fetching committed offsets for partitions: {}", groupId, partitions);
        // construct the request
        OffsetFetchRequest.Builder requestBuilder =
                new OffsetFetchRequest.Builder(this.groupId, new ArrayList<>(partitions));

        // send the request with a callback
        return client.send(coordinator, requestBuilder)
                .compose(new OffsetFetchResponseHandler());
    }

    private class OffsetFetchResponseHandler extends CoordinatorResponseHandler<OffsetFetchResponse, Map<TopicPartition, OffsetAndMetadata>> {
        @Override
        public void handle(OffsetFetchResponse response, RequestFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
            if (response.hasError()) {
                Errors error = response.error();
                log.debug("Offset fetch for group {} failed: {}", groupId, error.message());

                if (error == Errors.GROUP_LOAD_IN_PROGRESS) {
                    // just retry
                    future.raise(error);
                } else if (error == Errors.NOT_COORDINATOR_FOR_GROUP) {
                    // re-discover the coordinator and retry
                    coordinatorDead();
                    future.raise(error);
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(new GroupAuthorizationException(groupId));
                } else {
                    future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                }
                return;
            }

            Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>(response.responseData().size());
            for (Map.Entry<TopicPartition, OffsetFetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                TopicPartition tp = entry.getKey();
                OffsetFetchResponse.PartitionData data = entry.getValue();
                if (data.hasError()) {
                    Errors error = data.error;
                    log.debug("Group {} failed to fetch offset for partition {}: {}", groupId, tp, error.message());

                    if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                        future.raise(new KafkaException("Partition " + tp + " may not exist or the user may not have " +
                                "Describe access to the topic"));
                    } else {
                        future.raise(new KafkaException("Unexpected error in fetch offset response: " + error.message()));
                    }
                    return;
                } else if (data.offset >= 0) {
                    // record the position with the offset (-1 indicates no committed offset to fetch)
                    offsets.put(tp, new OffsetAndMetadata(data.offset, data.metadata));
                } else {
                    log.debug("Group {} has no committed offset for partition {}", groupId, tp);
                }
            }

            future.complete(offsets);
        }
    }

    private class ConsumerCoordinatorMetrics {
        private final String metricGrpName;
        private final Sensor commitLatency;

        private ConsumerCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.commitLatency = metrics.sensor("commit-latency");
            this.commitLatency.add(metrics.metricName("commit-latency-avg",
                this.metricGrpName,
                "The average time taken for a commit request"), new Avg());
            this.commitLatency.add(metrics.metricName("commit-latency-max",
                this.metricGrpName,
                "The max time taken for a commit request"), new Max());
            this.commitLatency.add(metrics.metricName("commit-rate",
                this.metricGrpName,
                "The number of commit calls per second"), new Rate(new Count()));

            Measurable numParts =
                new Measurable() {
                    public double measure(MetricConfig config, long now) {
                        return subscriptions.assignedPartitions().size();
                    }
                };
            metrics.addMetric(metrics.metricName("assigned-partitions",
                this.metricGrpName,
                "The number of partitions currently assigned to this consumer"), numParts);
        }
    }

    private static class MetadataSnapshot {
        private final Map<String, Integer> partitionsPerTopic;

        private MetadataSnapshot(SubscriptionState subscription, Cluster cluster) {
            Map<String, Integer> partitionsPerTopic = new HashMap<>();
            for (String topic : subscription.groupSubscription())
                partitionsPerTopic.put(topic, cluster.partitionCountForTopic(topic));
            this.partitionsPerTopic = partitionsPerTopic;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MetadataSnapshot that = (MetadataSnapshot) o;
            return partitionsPerTopic != null ? partitionsPerTopic.equals(that.partitionsPerTopic) : that.partitionsPerTopic == null;
        }

        @Override
        public int hashCode() {
            return partitionsPerTopic != null ? partitionsPerTopic.hashCode() : 0;
        }
    }

    private static class OffsetCommitCompletion {
        private final OffsetCommitCallback callback;
        private final Map<TopicPartition, OffsetAndMetadata> offsets;
        private final Exception exception;

        private OffsetCommitCompletion(OffsetCommitCallback callback, Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
            this.callback = callback;
            this.offsets = offsets;
            this.exception = exception;
        }

        public void invoke() {
            if (callback != null)
                callback.onComplete(offsets, exception);
        }
    }

}
