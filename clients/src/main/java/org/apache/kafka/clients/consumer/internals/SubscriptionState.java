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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.PartitionStates;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * A class for tracking the topics, partitions, and offsets for the consumer. A partition
 * is "assigned" either directly with {@link #assignFromUser(Set)} (manual assignment)
 * or with {@link #assignFromSubscribed(Collection)} (automatic assignment from subscription).
 *
 * Once assigned, the partition is not considered "fetchable" until its initial position has
 * been set with {@link #seek(TopicPartition, long)}. Fetchable partitions track a fetch
 * position which is used to set the offset of the next fetch, and a consumed position
 * which is the last offset that has been returned to the user. You can suspend fetching
 * from a partition through {@link #pause(TopicPartition)} without affecting the fetched/consumed
 * offsets. The partition will remain unfetchable until the {@link #resume(TopicPartition)} is
 * used. You can also query the pause state independently with {@link #isPaused(TopicPartition)}.
 *
 * Note that pause state as well as fetch/consumed positions are not preserved when partition
 * assignment is changed whether directly by the user or through a group rebalance.
 *
 * This class also maintains a cache of the latest commit position for each of the assigned
 * partitions. This is updated through {@link #committed(TopicPartition, OffsetAndMetadata)} and can be used
 * to set the initial fetch position (e.g. {@link Fetcher#resetOffset(TopicPartition)}.
 */
public class SubscriptionState {
    private static final String SUBSCRIPTION_EXCEPTION_MESSAGE =
            "Subscription to topics, partitions and pattern are mutually exclusive";

    // 订阅模式枚举
    private enum SubscriptionType {
        NONE, // 初始值
        AUTO_TOPICS,// 根据指定的Topic名字进行订阅，自动分配分区
        AUTO_PATTERN,// 按照指定的正则表达式匹配Topic进行订阅，自动分配分区
        USER_ASSIGNED// 用户手动指定消费者消费的Topic及分区编号
    }

    /* the type of subscription */
    // 订阅模式
    private SubscriptionType subscriptionType;

    /* the pattern user has requested */
    // 使用AUTO_PATTERN正则匹配时，该字段记录了正则表达式
    private Pattern subscribedPattern;


    /* the list of topics the user has requested */
    // 使用AUTO_TOPICS或AUTO_PATTERN模式时，使用该集合记录所有订阅的Topic
    private Set<String> subscription;

    /* the list of topics the group has subscribed to (set only for the leader on join group completion) */
    // Group Leader使用该集合记录Group中所有消费者订阅的Topic，其他Follower只记录了自己订阅的Topic
    private final Set<String> groupSubscription;

    /* the partitions that are currently assigned, note that the order of partition matters (see FetchBuilder for more details) */
    // 使用USER_ASSIGNED模式时，此集合记录了分配给当前消费者的TopicPartition集合，也会记录订阅模式下由协调者分配的TopicPartition集合
    private final PartitionStates<TopicPartitionState> assignment;

    /* do we need to request the latest committed offsets from the coordinator? */
    /**
     * 是否需要从GroupCoordinator获取最近提交的offset，
     * 当出现异步提交offset操作或者Rebalance操作刚完成时会将其设置为true，
     * 成功获取最近提交的offset之后会设置为false
     */
    private boolean needsFetchCommittedOffsets;

    /* Default offset reset strategy */
    // 默认的Offset重置策略
    private final OffsetResetStrategy defaultResetStrategy;

    /* User-provided listener to be invoked when assignment changes */
    // 用于监听分区分配操作的监听器
    private ConsumerRebalanceListener listener;

    /* Listeners provide a hook for internal state cleanup (e.g. metrics) on assignment changes */
    private List<Listener> listeners = new ArrayList<>();

    public SubscriptionState(OffsetResetStrategy defaultResetStrategy) {
        this.defaultResetStrategy = defaultResetStrategy;
        this.subscription = Collections.emptySet();
        this.assignment = new PartitionStates<>();
        this.groupSubscription = new HashSet<>();
        this.needsFetchCommittedOffsets = true; // initialize to true for the consumers to fetch offset upon starting up
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
    }

    /**
     * This method sets the subscription type if it is not already set (i.e. when it is NONE),
     * or verifies that the subscription type is equal to the give type when it is set (i.e.
     * when it is not NONE)
     * @param type The given subscription type
     */
    private void setSubscriptionType(SubscriptionType type) {
        // 只有在NONE模式下才可以指定为其他格式
        if (this.subscriptionType == SubscriptionType.NONE)
            this.subscriptionType = type;
        else if (this.subscriptionType != type)
            // 如果已经设置过一次，再次设置为不同的模式会报错
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
    }

    // 根据主题集合及特定的重均衡监听器来订阅主题
    public void subscribe(Set<String> topics, ConsumerRebalanceListener listener) {
        // 指定的重均衡监听器不可为空
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");
        // 设置订阅模式
        setSubscriptionType(SubscriptionType.AUTO_TOPICS);

        this.listener = listener;
        // 修改subscription字段，记录订阅的分区
        changeSubscription(topics);
    }


    public void subscribeFromPattern(Set<String> topics) {

        if (subscriptionType != SubscriptionType.AUTO_PATTERN)
            throw new IllegalArgumentException("Attempt to subscribe from pattern while subscription type set to " +
                    subscriptionType);
        // 更新subscriptions集合、groupSubscription集合、assignment集合
        changeSubscription(topics);
    }

    // 改变subscription字段
    private void changeSubscription(Set<String> topicsToSubscribe) {
        // 订阅的主题有变化
        if (!this.subscription.equals(topicsToSubscribe)) {
            this.subscription = topicsToSubscribe;
            this.groupSubscription.addAll(topicsToSubscribe);
        }
    }

    /**
     * Add topics to the current group subscription. This is used by the group leader to ensure
     * that it receives metadata updates for all topics that the group is interested in.
     * @param topics The topics to add to the group subscription
     */
    public void groupSubscribe(Collection<String> topics) {
        // 检查订阅模式是否是USER_ASSIGINED，如果是则抛出异常
        if (this.subscriptionType == SubscriptionType.USER_ASSIGNED)
            throw new IllegalStateException(SUBSCRIPTION_EXCEPTION_MESSAGE);
        // 将传入的主题添加到groupSubscription中进行记录
        this.groupSubscription.addAll(topics);
    }

    /**
     * Reset the group's subscription to only contain topics subscribed by this consumer.
     */
    public void resetGroupSubscription() {
        this.groupSubscription.retainAll(subscription);
    }

    /**
     * Change the assignment to the specified partitions provided by the user,
     * note this is different from {@link #assignFromSubscribed(Collection)}
     * whose input partitions are provided from the subscribed topics.
     */
    public void assignFromUser(Set<TopicPartition> partitions) {
        setSubscriptionType(SubscriptionType.USER_ASSIGNED);

        if (!this.assignment.partitionSet().equals(partitions)) {
            fireOnAssignment(partitions);

            Map<TopicPartition, TopicPartitionState> partitionToState = new HashMap<>();
            for (TopicPartition partition : partitions) {
                TopicPartitionState state = assignment.stateValue(partition);
                if (state == null)
                    state = new TopicPartitionState();
                partitionToState.put(partition, state);
            }
            this.assignment.set(partitionToState);
            this.needsFetchCommittedOffsets = true;
        }
    }

    /**
     * Change the assignment to the specified partitions returned from the coordinator,
     * note this is different from {@link #assignFromUser(Set)} which directly set the assignment from user inputs
     */
    // 消费者分配到分区后，会调用该方法，将分区及其状态添加到assignment
    public void assignFromSubscribed(Collection<TopicPartition> assignments) {
        if (!this.partitionsAutoAssigned())
            throw new IllegalArgumentException("Attempt to dynamically assign partitions while manual assignment in use");

        Map<TopicPartition, TopicPartitionState> assignedPartitionStates = partitionToStateMap(assignments);
        fireOnAssignment(assignedPartitionStates.keySet());

        if (this.subscribedPattern != null) {

            for (TopicPartition tp : assignments) {
                if (!this.subscribedPattern.matcher(tp.topic()).matches())
                    throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic regex pattern; subscription pattern is " + this.subscribedPattern);
            }
        } else {
            // 遍历传入的assignments，判断当前subscription是否包含指定的主题
            for (TopicPartition tp : assignments)
                if (!this.subscription.contains(tp.topic()))
                    throw new IllegalArgumentException("Assigned partition " + tp + " for non-subscribed topic; subscription is " + this.subscription);
        }

        // after rebalancing, we always reinitialize the assignment value
        this.assignment.set(assignedPartitionStates);
        this.needsFetchCommittedOffsets = true;
    }

    // 根据正则表达式匹配主题，及特定的重均衡监听器来订阅主题
    public void subscribe(Pattern pattern, ConsumerRebalanceListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("RebalanceListener cannot be null");

        // 设置订阅模式
        setSubscriptionType(SubscriptionType.AUTO_PATTERN);
        // 记录参数
        this.listener = listener;
        this.subscribedPattern = pattern;
    }

    public boolean hasPatternSubscription() {
        return this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public boolean hasNoSubscriptionOrUserAssignment() {
        return this.subscriptionType == SubscriptionType.NONE;
    }

    public void unsubscribe() {
        this.subscription = Collections.emptySet();
        this.assignment.clear();
        this.subscribedPattern = null;
        this.subscriptionType = SubscriptionType.NONE;
        fireOnAssignment(Collections.<TopicPartition>emptySet());
    }

    public Pattern subscribedPattern() {
        return this.subscribedPattern;
    }

    public Set<String> subscription() {
        return this.subscription;
    }

    public Set<TopicPartition> pausedPartitions() {
        HashSet<TopicPartition> paused = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (state.value().paused) {
                paused.add(state.topicPartition());
            }
        }
        return paused;
    }

    /**
     * Get the subscription for the group. For the leader, this will include the union of the
     * subscriptions of all group members. For followers, it is just that member's subscription.
     * This is used when querying topic metadata to detect the metadata changes which would
     * require rebalancing. The leader fetches metadata for all topics in the group so that it
     * can do the partition assignment (which requires at least partition counts for all topics
     * to be assigned).
     * @return The union of all subscribed topics in the group if this member is the leader
     *   of the current generation; otherwise it returns the same set as {@link #subscription()}
     */
    public Set<String> groupSubscription() {
        return this.groupSubscription;
    }

    // 获取tp对应的TopicPartitionState对象
    private TopicPartitionState assignedState(TopicPartition tp) {
        // 从assignment字典中根据键获取
        TopicPartitionState state = this.assignment.stateValue(tp);
        if (state == null)
            throw new IllegalStateException("No current assignment for partition " + tp);
        return state;
    }

    public void committed(TopicPartition tp, OffsetAndMetadata offset) {
        assignedState(tp).committed(offset);
    }

    public OffsetAndMetadata committed(TopicPartition tp) {
        return assignedState(tp).committed;
    }

    public void needRefreshCommits() {
        this.needsFetchCommittedOffsets = true;
    }

    public boolean refreshCommitsNeeded() {
        return this.needsFetchCommittedOffsets;
    }

    public void commitsRefreshed() {
        this.needsFetchCommittedOffsets = false;
    }

    // 更新tp分区的position为offset
    public void seek(TopicPartition tp, long offset) {
        assignedState(tp).seek(offset);
    }

    public Set<TopicPartition> assignedPartitions() {
        return this.assignment.partitionSet();
    }

    // 获取分配给当前消费者的可拉取分区的信息
    public List<TopicPartition> fetchablePartitions() {
        List<TopicPartition> fetchable = new ArrayList<>(assignment.size());
        // 遍历assignment
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            /**
             * 判断是否可以拉取，isFetchable()为true有两个条件
             * 1. 对应的TopicPartition未被标记为暂停状态；
             * 2. 对应的TopicPartitionState的position不为null
             */
            if (state.value().isFetchable())
                fetchable.add(state.topicPartition());
        }
        return fetchable;
    }

    public boolean partitionsAutoAssigned() {
        return this.subscriptionType == SubscriptionType.AUTO_TOPICS || this.subscriptionType == SubscriptionType.AUTO_PATTERN;
    }

    public void position(TopicPartition tp, long offset) {
        assignedState(tp).position(offset);
    }

    public Long position(TopicPartition tp) {
        return assignedState(tp).position;
    }

    public Long partitionLag(TopicPartition tp) {
        TopicPartitionState topicPartitionState = assignedState(tp);
        return topicPartitionState.highWatermark == null ? null : topicPartitionState.highWatermark - topicPartitionState.position;
    }

    public void updateHighWatermark(TopicPartition tp, long highWatermark) {
        assignedState(tp).highWatermark = highWatermark;
    }

    public Map<TopicPartition, OffsetAndMetadata> allConsumed() {
        Map<TopicPartition, OffsetAndMetadata> allConsumed = new HashMap<>();
        // 遍历assignment
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            // 获取主题分区状态
            if (state.value().hasValidPosition())
                // 如果状态中记录了下次要从Kafka服务端获取的消息的offset，就将其添加到allConsumed中
                allConsumed.put(state.topicPartition(), new OffsetAndMetadata(state.value().position));
        }
        return allConsumed;
    }

    public void needOffsetReset(TopicPartition partition, OffsetResetStrategy offsetResetStrategy) {
        assignedState(partition).awaitReset(offsetResetStrategy);
    }

    // 使用默认策略更新position
    public void needOffsetReset(TopicPartition partition) {
        needOffsetReset(partition, defaultResetStrategy);
    }

    public boolean hasDefaultOffsetResetPolicy() {
        return defaultResetStrategy != OffsetResetStrategy.NONE;
    }

    public boolean isOffsetResetNeeded(TopicPartition partition) {
        return assignedState(partition).awaitingReset();
    }

    public OffsetResetStrategy resetStrategy(TopicPartition partition) {
        return assignedState(partition).resetStrategy;
    }

    /**
     * 判断是否所有分区都存在有效的偏移量
     */
    public boolean hasAllFetchPositions(Collection<TopicPartition> partitions) {
        for (TopicPartition partition : partitions)
            if (!hasValidPosition(partition))
                return false;
        return true;
    }

    public boolean hasAllFetchPositions() {
        return hasAllFetchPositions(this.assignedPartitions());
    }

    // 筛选position未知的所有分区
    public Set<TopicPartition> missingFetchPositions() {
        Set<TopicPartition> missing = new HashSet<>();
        for (PartitionStates.PartitionState<TopicPartitionState> state : assignment.partitionStates()) {
            if (!state.value().hasValidPosition())
                missing.add(state.topicPartition());
        }
        return missing;
    }

    public boolean isAssigned(TopicPartition tp) {
        return assignment.contains(tp);
    }

    public boolean isPaused(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).paused;
    }

    public boolean isFetchable(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).isFetchable();
    }

    public boolean hasValidPosition(TopicPartition tp) {
        return isAssigned(tp) && assignedState(tp).hasValidPosition();
    }

    public void pause(TopicPartition tp) {
        // 获取tp对应的TopicPartitionState，调用pause()方法
        assignedState(tp).pause();
    }

    public void resume(TopicPartition tp) {
        assignedState(tp).resume();
    }

    public void movePartitionToEnd(TopicPartition tp) {
        assignment.moveToEnd(tp);
    }

    public ConsumerRebalanceListener listener() {
        return listener;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void fireOnAssignment(Set<TopicPartition> assignment) {
        for (Listener listener : listeners)
            listener.onAssignment(assignment);
    }

    private static Map<TopicPartition, TopicPartitionState> partitionToStateMap(Collection<TopicPartition> assignments) {
        Map<TopicPartition, TopicPartitionState> map = new HashMap<>(assignments.size());
        for (TopicPartition tp : assignments)
            map.put(tp, new TopicPartitionState());
        return map;
    }

    // 表示TopicPartition的消费状态
    private static class TopicPartitionState {
        // 拉取偏移量  记录了下次要从Kafka服务端获取的消息的offset
        private Long position; // last consumed position

        private Long highWatermark; // the high watermark from last fetch
        // 消费偏移量  提交偏移量  记录了最近一次提交的offset
        private OffsetAndMetadata committed;  // last committed position
        // 记录了当前TopicPartition是否处于暂停状态，用于Consumer接口的pause()方法
        private boolean paused;  // whether this partition has been paused by the user
        // 重置position的策略，该字段是否为空代表是否需要重置position的值
        private OffsetResetStrategy resetStrategy;  // the strategy to use if the offset needs resetting

        public TopicPartitionState() {
            this.paused = false;
            this.position = null;
            this.highWatermark = null;
            this.committed = null;
            this.resetStrategy = null;
        }

        // 重置拉取偏移量 第一次分配给消费者时调用
        private void awaitReset(OffsetResetStrategy strategy) {
            this.resetStrategy = strategy;  // 设置重置策略
            this.position = null;   // 清空position
        }

        public boolean awaitingReset() {
            return resetStrategy != null;
        }

        // position是否合法
        public boolean hasValidPosition() {
            return position != null;
        }

        // 开始重置 设置下次要从Kafka服务端获取的消息的offset
        private void seek(long offset) {
            this.position = offset; // 设置position
            this.resetStrategy = null;  // 清空重置策略
        }

        // 设置position值 更新拉取偏移量（拉取线程在拉取到消息后调用）
        private void position(long offset) {
            // 只有在position合法时才可以设置
            if (!hasValidPosition())
                throw new IllegalStateException("Cannot set a new position without a valid current position");
            this.position = offset;
        }

        // 设置最近一次提交的offset 更新提交偏移量（定时提交任务调用）
        private void committed(OffsetAndMetadata offset) {
            this.committed = offset;
        }

        // 暂停当前主题分区的消费
        private void pause() {
            this.paused = true;
        }

        // 重启当前主题分区的消费
        private void resume() {
            this.paused = false;
        }

        /**
         * 判断是否可以拉取，isFetchable()为true有两个条件
         * 1. 对应的TopicPartition未被标记为暂停状态；
         * 2. 对应的TopicPartitionState的position不为null
         */
        private boolean isFetchable() {
            return !paused && hasValidPosition();
        }

    }

    public interface Listener {
        /**
         * Fired after a new assignment is received (after a group rebalance or when the user manually changes the
         * assignment).
         *
         * @param assignment The topic partitions assigned to the consumer
         */
        void onAssignment(Set<TopicPartition> assignment);
    }

}
