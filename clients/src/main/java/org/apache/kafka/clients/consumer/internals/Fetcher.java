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

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.Value;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.record.LogEntry;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.FetchResponse;
import org.apache.kafka.common.requests.ListOffsetRequest;
import org.apache.kafka.common.requests.ListOffsetResponse;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class manage the fetching process with the brokers.
 */
public class Fetcher<K, V> implements SubscriptionState.Listener {

    private static final Logger log = LoggerFactory.getLogger(Fetcher.class);

    // 负责网络通信
    private final ConsumerNetworkClient client;
    private final Time time;
    // 在服务端收到FetchRequest之后并不是立即响应，而是当可返回的消息数据积累到至少minBytes个字节时才进行响应。
    // 每个FetchResponse中包含多条消息，提高网络的有效负载
    private final int minBytes;
    // 等待FetchResponse的最长时间，服务端根据此时间决定何时进行响应
    private final int maxBytes;

    private final int maxWaitMs;
    // 每次fetch操作的最大字节数
    private final int fetchSize;
    // 重试退避时间
    private final long retryBackoffMs;
    // 每次获取Record的最大数量
    private final int maxPollRecords;
    private final boolean checkCrcs;
    // 集群元数据
    private final Metadata metadata;
    private final FetchManagerMetrics sensors;
    // 记录了每个TopicPartition的消费情况
    private final SubscriptionState subscriptions;
    // 每个FetchResponse首先会转换为CompletedFetch对象进入此队列缓存
    private final ConcurrentLinkedQueue<CompletedFetch> completedFetches;
    private final Deserializer<K> keyDeserializer;
    private final Deserializer<V> valueDeserializer;
    // 保存了CompletedFetch解析后的结果集合
    private PartitionRecords<K, V> nextInLineRecords = null;

    public Fetcher(ConsumerNetworkClient client,
                   int minBytes,
                   int maxBytes,
                   int maxWaitMs,
                   int fetchSize,
                   int maxPollRecords,
                   boolean checkCrcs,
                   Deserializer<K> keyDeserializer,
                   Deserializer<V> valueDeserializer,
                   Metadata metadata,
                   SubscriptionState subscriptions,
                   Metrics metrics,
                   String metricGrpPrefix,
                   Time time,
                   long retryBackoffMs) {
        this.time = time;
        this.client = client;
        this.metadata = metadata;
        this.subscriptions = subscriptions;
        this.minBytes = minBytes;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
        this.fetchSize = fetchSize;
        this.maxPollRecords = maxPollRecords;
        this.checkCrcs = checkCrcs;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.completedFetches = new ConcurrentLinkedQueue<>();
        this.sensors = new FetchManagerMetrics(metrics, metricGrpPrefix);
        this.retryBackoffMs = retryBackoffMs;

        subscriptions.addListener(this);
    }

    /**
     * Represents data about an offset returned by a broker.
     */
    private static class OffsetData {
        /**
         * The offset
         */
        final long offset;

        /**
         * The timestamp.
         *
         * Will be null if the broker does not support returning timestamps.
         */
        final Long timestamp;

        OffsetData(long offset, Long timestamp) {
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }

    /**
     * Return whether we have any completed fetches pending return to the user. This method is thread-safe.
     * @return true if there are completed fetches, false otherwise
     */
    public boolean hasCompletedFetches() {
        return !completedFetches.isEmpty();
    }

    private boolean matchesRequestedPartitions(FetchRequest.Builder request, FetchResponse response) {
        Set<TopicPartition> requestedPartitions = request.fetchData().keySet();
        Set<TopicPartition> fetchedPartitions = response.responseData().keySet();
        return fetchedPartitions.equals(requestedPartitions);
    }

    /**
     * Set-up a fetch request for any node that we have assigned partitions for which doesn't already have
     * an in-flight fetch or pending fetch data.
     * @return number of fetches sent
     * 发送拉取消息的请求
     */
    public int sendFetches() {
        // 封装请求
        Map<Node, FetchRequest.Builder> fetchRequestMap = createFetchRequests();
        // 遍历createFetchRequest()方法得到的待发送请求字典，类型Map<Node, FetchRequest>
        for (Map.Entry<Node, FetchRequest.Builder> fetchEntry : fetchRequestMap.entrySet()) {
            // 得到FetchRequest对象
            final FetchRequest.Builder request = fetchEntry.getValue();
            final Node fetchTarget = fetchEntry.getKey();

            log.debug("Sending fetch for partitions {} to broker {}", request.fetchData().keySet(), fetchTarget);
            // 使用ConsumerNetworkClient发送请求 FETCH请求
            client.send(fetchTarget, request)
                    // 并注册FetchResponse处理方法
                    .addListener(new RequestFutureListener<ClientResponse>() {
                        // 处理FetchResponse响应数据
                        @Override
                        public void onSuccess(ClientResponse resp) {
                            // 从ClientResponse响应对象数据组装FetchResponse对象
                            FetchResponse response = (FetchResponse) resp.responseBody();
                            /**
                             * 得到响应信息中所有的分区信息
                             * responseData()方法返回值类型为Map<TopicPartition, PartitionData>
                             */
                            if (!matchesRequestedPartitions(request, response)) {
                                // obviously we expect the broker to always send us valid responses, so this check
                                // is mainly for test cases where mock fetch responses must be manually crafted.
                                log.warn("Ignoring fetch response containing partitions {} since it does not match " +
                                        "the requested partitions {}", response.responseData().keySet(),
                                        request.fetchData().keySet());
                                return;
                            }

                            Set<TopicPartition> partitions = new HashSet<>(response.responseData().keySet());
                            FetchResponseMetricAggregator metricAggregator = new FetchResponseMetricAggregator(sensors, partitions);
                            // 遍历responseData()方法返回的响应信息
                            for (Map.Entry<TopicPartition, FetchResponse.PartitionData> entry : response.responseData().entrySet()) {
                                TopicPartition partition = entry.getKey();
                                // 从对应的FetchRequest中获取分区对应的offset
                                long fetchOffset = request.fetchData().get(partition).offset;
                                // 响应数据
                                FetchResponse.PartitionData fetchData = entry.getValue();
                                // 将响应数据构造为CompletedFetch对象添加到completedFetches集合
                                completedFetches.add(new CompletedFetch(partition, fetchOffset, fetchData, metricAggregator,
                                        request.version()));
                            }

                            sensors.fetchLatency.record(resp.requestLatencyMs());
                            sensors.fetchThrottleTimeSensor.record(response.getThrottleTime());
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            log.debug("Fetch request to {} for partitions {} failed", fetchTarget, request.fetchData().keySet(), e);
                        }
                    });
        }
        return fetchRequestMap.size();
    }

    /**
     * Lookup and set offsets for any partitions which are awaiting an explicit reset.
     * @param partitions the partitions to reset
     */
    public void resetOffsetsIfNeeded(Set<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            // TODO: If there are several offsets to reset, we could submit offset requests in parallel
            if (subscriptions.isAssigned(tp) && subscriptions.isOffsetResetNeeded(tp))
                resetOffset(tp);
        }
    }

    /**
     * Update the fetch positions for the provided partitions.
     * @param partitions the partitions to update positions for
     * @throws NoOffsetForPartitionException If no offset is stored for a given partition and no reset policy is available
     */
    public void updateFetchPositions(Set<TopicPartition> partitions) {
        // reset the fetch position to the committed position
        for (TopicPartition tp : partitions) {
            /**
             * 检测是否需要重置操作
             * 1. subscriptions.isAssigned(tp)：当前消费者是否被指定了tp分区
             * 2. subscriptions.isFetchable(tp)
             *      isAssigned(tp) ：当前消费者是否被指定了tp分区
             *      assignedState(tp).isFetchable()：当前分区是否是可以被拉取的
             *          !paused：当前分区是否是非暂停拉取状态的
             *          hasValidPosition()：position是否为null
             * 结果：
             * - 如果当前消费者没有被指定消费tp分区，那么不需要重置
             * - 如果当前消费者被指定消费tp分区，且tp分区是非暂停拉取状态，同时position值不为null，那么不需要重置
             */
            if (!subscriptions.isAssigned(tp) || subscriptions.hasValidPosition(tp))
                continue;

            // 走到这里说明需要重置position
            // 判断是否指定了重置策略
            if (subscriptions.isOffsetResetNeeded(tp)) {
                // 指定了重置策略，按照指定策略进行更新
                resetOffset(tp);
            } else if (subscriptions.committed(tp) == null) {
                // there's no committed position, so we need to reset with the default strategy
                // 最近一次该分区的提交为空，按照默认的defaultResetStrategy策略进行重置
                subscriptions.needOffsetReset(tp);
                resetOffset(tp);
            } else {
                // 最近一次该分区的提交不为空，则将position重置为最近一次提交的offset
                long committed = subscriptions.committed(tp).offset();
                log.debug("Resetting offset for partition {} to the committed offset {}", tp, committed);
                subscriptions.seek(tp, committed);
            }
        }
    }

    /**
     * Get topic metadata for all topics in the cluster
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getAllTopicMetadata(long timeout) {
        return getTopicMetadata(MetadataRequest.Builder.allTopics(), timeout);
    }

    /**
     * Get metadata for all topics present in Kafka cluster
     *
     * @param request The MetadataRequest to send
     * @param timeout time for which getting topic metadata is attempted
     * @return The map of topics with their partition information
     */
    public Map<String, List<PartitionInfo>> getTopicMetadata(MetadataRequest.Builder request, long timeout) {
        // Save the round trip if no topics are requested.
        if (!request.isAllTopics() && request.topics().isEmpty())
            return Collections.emptyMap();

        long start = time.milliseconds();
        long remaining = timeout;

        do {
            RequestFuture<ClientResponse> future = sendMetadataRequest(request);
            client.poll(future, remaining);

            if (future.failed() && !future.isRetriable())
                throw future.exception();

            if (future.succeeded()) {
                MetadataResponse response = (MetadataResponse) future.value().responseBody();
                Cluster cluster = response.cluster();

                Set<String> unauthorizedTopics = cluster.unauthorizedTopics();
                if (!unauthorizedTopics.isEmpty())
                    throw new TopicAuthorizationException(unauthorizedTopics);

                boolean shouldRetry = false;
                Map<String, Errors> errors = response.errors();
                if (!errors.isEmpty()) {
                    // if there were errors, we need to check whether they were fatal or whether
                    // we should just retry

                    log.debug("Topic metadata fetch included errors: {}", errors);

                    for (Map.Entry<String, Errors> errorEntry : errors.entrySet()) {
                        String topic = errorEntry.getKey();
                        Errors error = errorEntry.getValue();

                        if (error == Errors.INVALID_TOPIC_EXCEPTION)
                            throw new InvalidTopicException("Topic '" + topic + "' is invalid");
                        else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION)
                            // if a requested topic is unknown, we just continue and let it be absent
                            // in the returned map
                            continue;
                        else if (error.exception() instanceof RetriableException)
                            shouldRetry = true;
                        else
                            throw new KafkaException("Unexpected error fetching metadata for topic " + topic,
                                    error.exception());
                    }
                }

                if (!shouldRetry) {
                    HashMap<String, List<PartitionInfo>> topicsPartitionInfos = new HashMap<>();
                    for (String topic : cluster.topics())
                        topicsPartitionInfos.put(topic, cluster.availablePartitionsForTopic(topic));
                    return topicsPartitionInfos;
                }
            }

            long elapsed = time.milliseconds() - start;
            remaining = timeout - elapsed;

            if (remaining > 0) {
                long backoff = Math.min(remaining, retryBackoffMs);
                time.sleep(backoff);
                remaining -= backoff;
            }
        } while (remaining > 0);

        throw new TimeoutException("Timeout expired while fetching topic metadata");
    }

    /**
     * Send Metadata Request to least loaded node in Kafka cluster asynchronously
     * @return A future that indicates result of sent metadata request
     */
    private RequestFuture<ClientResponse> sendMetadataRequest(MetadataRequest.Builder request) {
        final Node node = client.leastLoadedNode();
        if (node == null)
            return RequestFuture.noBrokersAvailable();
        else
            return client.send(node, request);
    }

    /**
     * Reset offsets for the given partition using the offset reset strategy.
     *
     * @param partition The given partition that needs reset offset
     * @throws org.apache.kafka.clients.consumer.NoOffsetForPartitionException If no offset reset strategy is defined
     *
     * 根据重置策略更新分区的position
     */
    private void resetOffset(TopicPartition partition) {
        // 获取分区对应的重置策略
        OffsetResetStrategy strategy = subscriptions.resetStrategy(partition);
        log.debug("Resetting offset for partition {} to {} offset.", partition, strategy.name().toLowerCase(Locale.ROOT));
        final long timestamp;
        // 根据重置策略选择时间戳
        if (strategy == OffsetResetStrategy.EARLIEST)
            // 最早策略
            timestamp = ListOffsetRequest.EARLIEST_TIMESTAMP;
        else if (strategy == OffsetResetStrategy.LATEST)
            // 最晚策略
            timestamp = ListOffsetRequest.LATEST_TIMESTAMP;
        else
            throw new NoOffsetForPartitionException(partition);
        // 向分区的Leader副本所在的节点发送OffsetsRequest请求，得到对应的offset，会阻塞
        Map<TopicPartition, OffsetData> offsetsByTimes = retrieveOffsetsByTimes(
                Collections.singletonMap(partition, timestamp), Long.MAX_VALUE, false);
        OffsetData offsetData = offsetsByTimes.get(partition);
        if (offsetData == null)
            throw new NoOffsetForPartitionException(partition);
        long offset = offsetData.offset;
        // we might lose the assignment while fetching the offset, so check it is still active
        // 更新分区的position为得到的offset
        if (subscriptions.isAssigned(partition))
            this.subscriptions.seek(partition, offset);
    }

    public Map<TopicPartition, OffsetAndTimestamp> getOffsetsByTimes(Map<TopicPartition, Long> timestampsToSearch,
                                                                     long timeout) {
        Map<TopicPartition, OffsetData> offsetData = retrieveOffsetsByTimes(timestampsToSearch, timeout, true);
        HashMap<TopicPartition, OffsetAndTimestamp> offsetsByTimes = new HashMap<>(offsetData.size());
        for (Map.Entry<TopicPartition, OffsetData> entry : offsetData.entrySet()) {
            OffsetData data = entry.getValue();
            if (data == null)
                offsetsByTimes.put(entry.getKey(), null);
            else
                offsetsByTimes.put(entry.getKey(), new OffsetAndTimestamp(data.offset, data.timestamp));
        }
        return offsetsByTimes;
    }

    private Map<TopicPartition, OffsetData> retrieveOffsetsByTimes(
            Map<TopicPartition, Long> timestampsToSearch, long timeout, boolean requireTimestamps) {
        if (timestampsToSearch.isEmpty())
            return Collections.emptyMap();

        long startMs = time.milliseconds();
        long remaining = timeout;
        do {
            // 构造OffsetsRequest请求到unsent，等待发送
            RequestFuture<Map<TopicPartition, OffsetData>> future =
                    sendListOffsetRequests(requireTimestamps, timestampsToSearch);
            // 发送请求
            client.poll(future, remaining);

            if (!future.isDone())
                break;
            // 请求成功
            if (future.succeeded())
                return future.value();

            if (!future.isRetriable())
                throw future.exception();

            long elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
            if (remaining <= 0)
                break;

            if (future.exception() instanceof InvalidMetadataException)
                // 元数据无效异常，更新元数据
                client.awaitMetadataUpdate(remaining);
            else
                // 退避一段时间后再次请求
                time.sleep(Math.min(remaining, retryBackoffMs));

            elapsed = time.milliseconds() - startMs;
            remaining = timeout - elapsed;
        } while (remaining > 0);
        throw new TimeoutException("Failed to get offsets by times in " + timeout + " ms");
    }

    public Map<TopicPartition, Long> beginningOffsets(Collection<TopicPartition> partitions, long timeout) {
        return beginningOrEndOffset(partitions, ListOffsetRequest.EARLIEST_TIMESTAMP, timeout);
    }

    public Map<TopicPartition, Long> endOffsets(Collection<TopicPartition> partitions, long timeout) {
        return beginningOrEndOffset(partitions, ListOffsetRequest.LATEST_TIMESTAMP, timeout);
    }

    private Map<TopicPartition, Long> beginningOrEndOffset(Collection<TopicPartition> partitions,
                                                           long timestamp,
                                                           long timeout) {
        Map<TopicPartition, Long> timestampsToSearch = new HashMap<>();
        for (TopicPartition tp : partitions)
            timestampsToSearch.put(tp, timestamp);
        Map<TopicPartition, Long> result = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetData> entry :
                retrieveOffsetsByTimes(timestampsToSearch, timeout, false).entrySet()) {
            result.put(entry.getKey(), entry.getValue().offset);
        }
        return result;
    }

    /**
     * Return the fetched records, empty the record buffer and update the consumed position.
     *
     * NOTE: returning empty records guarantees the consumed position are NOT updated.
     *
     * @return The fetched records per partition
     * @throws OffsetOutOfRangeException If there is OffsetOutOfRange error in fetchResponse and
     *         the defaultResetPolicy is NONE
     */
    public Map<TopicPartition, List<ConsumerRecord<K, V>>> fetchedRecords() {
        // 遍历completedFetches，按照TopicPartition进行分类
        Map<TopicPartition, List<ConsumerRecord<K, V>>> drained = new HashMap<>();
        // 一次最大的获取Record数量
        int recordsRemaining = maxPollRecords;

        // 当还可以继续获取时
        while (recordsRemaining > 0) {
            if (nextInLineRecords == null || nextInLineRecords.isDrained()) {
                CompletedFetch completedFetch = completedFetches.poll();
                if (completedFetch == null)
                    break;

                nextInLineRecords = parseCompletedFetch(completedFetch);
            } else {
                TopicPartition partition = nextInLineRecords.partition;
                List<ConsumerRecord<K, V>> records = drainRecords(nextInLineRecords, recordsRemaining);
                if (!records.isEmpty()) {
                    List<ConsumerRecord<K, V>> currentRecords = drained.get(partition);
                    if (currentRecords == null) {
                        drained.put(partition, records);
                    } else {
                        // this case shouldn't usually happen because we only send one fetch at a time per partition,
                        // but it might conceivably happen in some rare cases (such as partition leader changes).
                        // we have to copy to a new list because the old one may be immutable
                        List<ConsumerRecord<K, V>> newRecords = new ArrayList<>(records.size() + currentRecords.size());
                        newRecords.addAll(currentRecords);
                        newRecords.addAll(records);
                        drained.put(partition, newRecords);
                    }
                    recordsRemaining -= records.size();
                }
            }
        }

        return drained;
    }

    // 将partitionRecords中的消息数据转换到drained中
    private List<ConsumerRecord<K, V>> drainRecords(PartitionRecords<K, V> partitionRecords, int maxRecords) {
        // 查看当前消费者是否被分配了对应的分区
        if (!subscriptions.isAssigned(partitionRecords.partition)) {
            // 当前消费者没有分配到该分区，这种情况一般是由于消息响应返回之前发生了Rebalance操作造成的
            // this can happen when a rebalance happened before fetched records are returned to the consumer's poll call
            log.debug("Not returning fetched records for partition {} since it is no longer assigned", partitionRecords.partition);
        } else {
            // note that the consumed position should always be available as long as the partition is still assigned
            // 获取当前subscriptions中存储的对应分区下次要从Kafka服务端获取的消息的offset
            long position = subscriptions.position(partitionRecords.partition);
            // 判断当前分区是否是可拉取的
            if (!subscriptions.isFetchable(partitionRecords.partition)) {
                // this can happen when a partition is paused before fetched records are returned to the consumer's poll call
                // 当前分区不可被拉取，这种情况一般是由于在消息响应返回之前分区被标记为暂停拉取而造成
                log.debug("Not returning fetched records for assigned partition {} since it is no longer fetchable", partitionRecords.partition);
            } else if (partitionRecords.fetchOffset == position) {
                // 拉取的消息的offset与之前暂存的position是相等的，说明消息被正确拉取了，且拉的是最新的记录
                // we are ensured to have at least one record since we already checked for emptiness
                // 从partitionRecords中取出maxRecords条记录，maxRecords指定了一次最多可以获取的消息数量
                List<ConsumerRecord<K, V>> partRecords = partitionRecords.drainRecords(maxRecords);
                if (!partRecords.isEmpty()) {
                    // 计算下一次的offset
                    long nextOffset = partRecords.get(partRecords.size() - 1).offset() + 1;
                    log.trace("Returning fetched records at offset {} for assigned partition {} and update " +
                            "position to {}", position, partitionRecords.partition, nextOffset);

                    subscriptions.position(partitionRecords.partition, nextOffset);
                }
                // 更新下次要从Kafka服务端获取的消息的offset
                Long partitionLag = subscriptions.partitionLag(partitionRecords.partition);
                if (partitionLag != null)
                    this.sensors.recordPartitionLag(partitionRecords.partition, partitionLag);
                // 返回消息记录
                return partRecords;
            } else {
                // these records aren't next in line based on the last consumed position, ignore them
                // they must be from an obsolete request
                log.debug("Ignoring fetched records for {} at offset {} since the current position is {}",
                        partitionRecords.partition, partitionRecords.fetchOffset, position);
            }
        }
        // 发生了Rebalance操作，将partitionRecords中的消息数据直接丢弃
        partitionRecords.drain();
        return Collections.emptyList();
    }

    /**
     * Search the offsets by target times for the specified partitions.
     *
     * @param requireTimestamps true if we should fail with an UnsupportedVersionException if the broker does
     *                         not support fetching precise timestamps for offsets
     * @param timestampsToSearch the mapping between partitions and target time
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetData>> sendListOffsetRequests(
            final boolean requireTimestamps,
            final Map<TopicPartition, Long> timestampsToSearch) {
        // Group the partitions by node.
        // 构造字典
        final Map<Node, Map<TopicPartition, Long>> timestampsToSearchByNode = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry: timestampsToSearch.entrySet()) {
            TopicPartition tp  = entry.getKey();
            // 从元数据中获取对应分区的信息
            PartitionInfo info = metadata.fetch().partition(tp);
            // 分区信息为null，说明元数据过期了
            if (info == null) {
                metadata.add(tp.topic());
                log.debug("Partition {} is unknown for fetching offset, wait for metadata refresh", tp);
                // 元数据过期，stale：陈腐的; 不新鲜的; 走了味的;
                return RequestFuture.staleMetadata();

                // 分区leader为null
            } else if (info.leader() == null) {
                log.debug("Leader for partition {} unavailable for fetching offset, wait for metadata refresh", tp);
                return RequestFuture.leaderNotAvailable();
            } else {
                // 得到分区的Leader节点
                Node node = info.leader();

                Map<TopicPartition, Long> topicData = timestampsToSearchByNode.get(node);

                if (topicData == null) {
                    topicData = new HashMap<>();
                    timestampsToSearchByNode.put(node, topicData);
                }
                topicData.put(entry.getKey(), entry.getValue());
            }
        }

        final RequestFuture<Map<TopicPartition, OffsetData>> listOffsetRequestsFuture = new RequestFuture<>();
        final Map<TopicPartition, OffsetData> fetchedTimestampOffsets = new HashMap<>();
        final AtomicInteger remainingResponses = new AtomicInteger(timestampsToSearchByNode.size());
        for (Map.Entry<Node, Map<TopicPartition, Long>> entry : timestampsToSearchByNode.entrySet()) {
            sendListOffsetRequest(entry.getKey(), entry.getValue(), requireTimestamps)
                    .addListener(new RequestFutureListener<Map<TopicPartition, OffsetData>>() {
                        @Override
                        public void onSuccess(Map<TopicPartition, OffsetData> value) {
                            synchronized (listOffsetRequestsFuture) {
                                fetchedTimestampOffsets.putAll(value);
                                if (remainingResponses.decrementAndGet() == 0 && !listOffsetRequestsFuture.isDone())
                                    listOffsetRequestsFuture.complete(fetchedTimestampOffsets);
                            }
                        }

                        @Override
                        public void onFailure(RuntimeException e) {
                            synchronized (listOffsetRequestsFuture) {
                                // This may cause all the requests to be retried, but should be rare.
                                if (!listOffsetRequestsFuture.isDone())
                                    listOffsetRequestsFuture.raise(e);
                            }
                        }
                    });
        }
        return listOffsetRequestsFuture;
    }

    /**
     * Send the ListOffsetRequest to a specific broker for the partitions and target timestamps.
     *
     * @param node The node to send the ListOffsetRequest to.
     * @param timestampsToSearch The mapping from partitions to the target timestamps.
     * @param requireTimestamp  True if we require a timestamp in the response.
     * @return A response which can be polled to obtain the corresponding timestamps and offsets.
     */
    private RequestFuture<Map<TopicPartition, OffsetData>> sendListOffsetRequest(final Node node,
                                                                                         final Map<TopicPartition, Long> timestampsToSearch,
                                                                                         boolean requireTimestamp) {
        ListOffsetRequest.Builder builder = new ListOffsetRequest.Builder().setTargetTimes(timestampsToSearch);

        // If we need a timestamp in the response, the minimum RPC version we can send is v1.
        // Otherwise, v0 is OK.
        builder.setMinVersion(requireTimestamp ? (short) 1 : (short) 0);

        log.trace("Sending ListOffsetRequest {} to broker {}", builder, node);
        return client.send(node, builder)
                .compose(new RequestFutureAdapter<ClientResponse, Map<TopicPartition, OffsetData>>() {
                    @Override
                    public void onSuccess(ClientResponse response, RequestFuture<Map<TopicPartition, OffsetData>> future) {
                        ListOffsetResponse lor = (ListOffsetResponse) response.responseBody();
                        log.trace("Received ListOffsetResponse {} from broker {}", lor, node);
                        // 处理响应数据
                        handleListOffsetResponse(timestampsToSearch, lor, future);
                    }
                });
    }

    /**
     * Callback for the response of the list offset call above.
     * @param timestampsToSearch The mapping from partitions to target timestamps
     * @param listOffsetResponse The response from the server.
     * @param future The future to be completed by the response.
     * 处理根据特定策略请求更新offset的响应
     */
    @SuppressWarnings("deprecation")
    private void handleListOffsetResponse(Map<TopicPartition, Long> timestampsToSearch,
                                          ListOffsetResponse listOffsetResponse,
                                          RequestFuture<Map<TopicPartition, OffsetData>> future) {
        Map<TopicPartition, OffsetData> timestampOffsetMap = new HashMap<>();
        for (Map.Entry<TopicPartition, Long> entry : timestampsToSearch.entrySet()) {
            TopicPartition topicPartition = entry.getKey();

            ListOffsetResponse.PartitionData partitionData = listOffsetResponse.responseData().get(topicPartition);
            // 获取错误码
            Errors error = Errors.forCode(partitionData.errorCode);
            if (error == Errors.NONE) {
                // 无错误码
                // 获取响应数据中相应分区的offset数据
                if (partitionData.offsets != null) {
                    // Handle v0 response
                    long offset;
                    if (partitionData.offsets.size() > 1) {
                        future.raise(new IllegalStateException("Unexpected partitionData response of length " +
                                partitionData.offsets.size()));
                        return;
                    } else if (partitionData.offsets.isEmpty()) {
                        offset = ListOffsetResponse.UNKNOWN_OFFSET;
                    } else {
                        offset = partitionData.offsets.get(0);
                    }
                    log.debug("Handling v0 ListOffsetResponse response for {}. Fetched offset {}",
                            topicPartition, offset);
                    if (offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                        OffsetData offsetData = new OffsetData(offset, null);
                        timestampOffsetMap.put(topicPartition, offsetData);
                    }
                } else {
                    // Handle v1 and later response
                    log.debug("Handling ListOffsetResponse response for {}. Fetched offset {}, timestamp {}",
                            topicPartition, partitionData.offset, partitionData.timestamp);
                    if (partitionData.offset != ListOffsetResponse.UNKNOWN_OFFSET) {
                        OffsetData offsetData = new OffsetData(partitionData.offset, partitionData.timestamp);
                        timestampOffsetMap.put(topicPartition, offsetData);
                    }
                }

                // 分区无Leader异常，未知的主题或分区异常
            } else if (error == Errors.UNSUPPORTED_FOR_MESSAGE_FORMAT) {
                // The message format on the broker side is before 0.10.0, we simply put null in the response.
                log.debug("Cannot search by timestamp for partition {} because the message format version " +
                        "is before 0.10.0", topicPartition);
                timestampOffsetMap.put(topicPartition, null);
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
                log.debug("Attempt to fetch offsets for partition {} failed due to obsolete leadership information, retrying.",
                        topicPartition);
                future.raise(error);
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in ListOffset request for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it", topicPartition);
                future.raise(error);
            } else {
                log.warn("Attempt to fetch offsets for partition {} failed due to: {}", topicPartition, error.message());
                future.raise(new StaleMetadataException());
            }
        }
        if (!future.isDone())
            future.complete(timestampOffsetMap);
    }

    private List<TopicPartition> fetchablePartitions() {
        Set<TopicPartition> exclude = new HashSet<>();
        // 获取所有分配给当前消费者的可拉取的分区
        List<TopicPartition> fetchable = subscriptions.fetchablePartitions();
        // 从fetchable移除nextInLineRecords记录中的分区，因为这些分区的响应还未被完全处理
        if (nextInLineRecords != null && !nextInLineRecords.isDrained()) {
            exclude.add(nextInLineRecords.partition);
        }
        // 从fetchable移除存在于completedFetches中分区，因为这些分区的响应还未被完全处理
        for (CompletedFetch completedFetch : completedFetches) {
            exclude.add(completedFetch.partition);
        }
        fetchable.removeAll(exclude);
        return fetchable;
    }

    /**
     * Create fetch requests for all nodes for which we have assigned partitions
     * that have no existing requests in flight.
     *
     * key是Node，value是发往对应Node的FetchRequest
     */
    private Map<Node, FetchRequest.Builder> createFetchRequests() {
        // create the fetch info
        // 获取集群元数据
        Cluster cluster = metadata.fetch();
        // 用于暂存需要发送的请求
        Map<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> fetchable = new LinkedHashMap<>();
        /**
         * 遍历可拉取的分区，这里可拉取的条件如下：
         * 1. 该分区是分配给当前消费者的；
         * 2. 该分区未被标记为暂停状态，同时对应的TopicPartitionState的position不为null；
         * 3. nextInLineRecords中没有来自此分区的消息；
         * 4. completedFetches队列，诶呦来自此分区的CompletedFetch。
         * 详见fetchablePartitions()方法
         */
        for (TopicPartition partition : fetchablePartitions()) {
            // 获取分区对应的Node信息
            Node node = cluster.leaderFor(partition);
            // 如果Node信息为null，强制更新集群元数据
            if (node == null) {
                metadata.requestUpdate();
            } else if (this.client.pendingRequestCount(node) == 0) {
                // if there is a leader and no in-flight requests, issue a new fetch
                // 如果没有正在发往该Node的请求
                // 从fetchable中查询node对应的Map<TopicPartition, FetchRequest.PartitionData>数据
                LinkedHashMap<TopicPartition, FetchRequest.PartitionData> fetch = fetchable.get(node);
                // 如果查找的是null，就新建一个
                if (fetch == null) {
                    fetch = new LinkedHashMap<>();
                    fetchable.put(node, fetch);
                }

                // 获取该分区当前的position，该position记录了下次要从Kafka服务端获取的消息的offset
                long position = this.subscriptions.position(partition);
                // 创建PartitionData对象，添加到对应的fetch字典中
                fetch.put(partition, new FetchRequest.PartitionData(position, this.fetchSize));
                log.trace("Added fetch request for partition {} at offset {} to node {}", partition, position, node);
            } else {
                log.trace("Skipping fetch for partition {} because there is an in-flight request to {}", partition, node);
            }
        }

        // create the fetches
        // 创建存储FetchRequest的字典，键为Node，值为发往该Node的FetchRequest
        Map<Node, FetchRequest.Builder> requests = new HashMap<>();
        // 遍历fetchable字典
        for (Map.Entry<Node, LinkedHashMap<TopicPartition, FetchRequest.PartitionData>> entry : fetchable.entrySet()) {
            Node node = entry.getKey();
            // 创建FetchRequest
            FetchRequest.Builder fetch = new FetchRequest.Builder(this.maxWaitMs, this.minBytes, entry.getValue()).
                    setMaxBytes(this.maxBytes);
            requests.put(node, fetch);
        }
        return requests;
    }

    /**
     * The callback for fetch completion
     * parseCompletedFetch
     */
    private PartitionRecords<K, V> parseCompletedFetch(CompletedFetch completedFetch) {
        // 获取分区
        TopicPartition tp = completedFetch.partition;
        // 获取分区对应的数据
        FetchResponse.PartitionData partition = completedFetch.partitionData;
        // 获取拉取消息的offset，这个offset会与前面发送拉取请求时记录的position进行比较，以判断拉取到的消息的偏移量是否正确
        long fetchOffset = completedFetch.fetchedOffset;
        int bytes = 0;
        int recordsCount = 0;
        PartitionRecords<K, V> parsedRecords = null;
        Errors error = Errors.forCode(partition.errorCode);

        try {
            if (!subscriptions.isFetchable(tp)) {
                // 分区是不可被拉取的，这种情况可能出现在由于处于Rebalance操作中或分区被标记为停止的情况下
                // this can happen when a rebalance happened or a partition consumption paused
                // while fetch is still in-flight
                log.debug("Ignoring fetched records for partition {} since it is no longer fetchable", tp);
            } else if (error == Errors.NONE) {
                // 没有错误，解析数据
                // we are interested in this fetch only if the beginning offset matches the
                // current consumed position
                Long position = subscriptions.position(tp);
                // 检查是否和返回的fetchOffset相同，如果不同表示得到的响应数据与当初希望拉取的数据的偏移量不同，直接返回null
                if (position == null || position != fetchOffset) {
                    log.debug("Discarding stale fetch response for partition {} since its offset {} does not match " +
                            "the expected offset {}", tp, fetchOffset, position);
                    return null;
                }

                List<ConsumerRecord<K, V>> parsed = new ArrayList<>();
                boolean skippedRecords = false;
                // 遍历records
                for (LogEntry logEntry : partition.records.deepEntries()) {
                    // Skip the messages earlier than current position.
                    // 判断是否需要跳过比当前position还要早的消息
                    if (logEntry.offset() >= position) {
                        // 解析消息并添加到parsed集合中
                        parsed.add(parseRecord(tp, logEntry));
                        // 维护已读取的字节数记录
                        bytes += logEntry.sizeInBytes();
                    } else
                        // 跳过消息，将skippedRecords置为true
                        skippedRecords = true;
                }
                // 消息数量
                recordsCount = parsed.size();

                log.trace("Adding fetched record for partition {} with offset {} to buffered record list", tp, position);
                parsedRecords = new PartitionRecords<>(fetchOffset, tp, parsed);

                if (parsed.isEmpty() && !skippedRecords && (partition.records.sizeInBytes() > 0)) {

                    if (completedFetch.responseVersion < 3) {
                        // Implement the pre KIP-74 behavior of throwing a RecordTooLargeException.
                        Map<TopicPartition, Long> recordTooLargePartitions = Collections.singletonMap(tp, fetchOffset);
                        throw new RecordTooLargeException("There are some messages at [Partition=Offset]: " +
                                recordTooLargePartitions + " whose size is larger than the fetch size " + this.fetchSize +
                                " and hence cannot be returned. Please considering upgrading your broker to 0.10.1.0 or " +
                                "newer to avoid this issue. Alternately, increase the fetch size on the client (using " +
                                ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG + ")",
                                recordTooLargePartitions);
                    } else {
                        // This should not happen with brokers that support FetchRequest/Response V3 or higher (i.e. KIP-74)
                        throw new KafkaException("Failed to make progress reading messages at " + tp + "=" +
                            fetchOffset + ". Received a non-empty fetch response from the server, but no " +
                            "complete records were found.");
                    }
                }

                if (partition.highWatermark >= 0) {
                    log.trace("Received {} records in fetch response for partition {} with offset {}", parsed.size(), tp, position);
                    subscriptions.updateHighWatermark(tp, partition.highWatermark);
                }
            } else if (error == Errors.NOT_LEADER_FOR_PARTITION) {
                log.debug("Error in fetch for partition {}: {}", tp, error.exceptionName());
                // 没有找到分区对应的Leader，或者没有找到分区对应的主题，此时本地的集群元数据可能过期了，需要更新
                this.metadata.requestUpdate();
            } else if (error == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
                log.warn("Received unknown topic or partition error in fetch for partition {}. The topic/partition " +
                        "may not exist or the user may not have Describe access to it", tp);
                this.metadata.requestUpdate();
            } else if (error == Errors.OFFSET_OUT_OF_RANGE) {
                // 拉取消息的偏移量超出了允许范围，判断获取到的消息的偏移量是否与请求时保存的position相同
                if (fetchOffset != subscriptions.position(tp)) {
                    // 不相同，说明拉取的消息不匹配，直接丢弃消息
                    log.debug("Discarding stale fetch response for partition {} since the fetched offset {}" +
                            "does not match the current offset {}", tp, fetchOffset, subscriptions.position(tp));
                } else if (subscriptions.hasDefaultOffsetResetPolicy()) {
                    // 相同，说明此时拉取的消息的偏移量确实超出范围了，如果SubscriptionState设置了偏移量重置策略，则标记需要重置偏移量
                    log.info("Fetch offset {} is out of range for partition {}, resetting offset", fetchOffset, tp);
                    subscriptions.needOffsetReset(tp);
                } else {
                    // 相同，说明此时拉取的消息的偏移量确实超出范围了，但没有设置偏移量重置策略，则抛出异常
                    throw new OffsetOutOfRangeException(Collections.singletonMap(tp, fetchOffset));
                }
            } else if (error == Errors.TOPIC_AUTHORIZATION_FAILED) {
                // 主题授权失败异常，抛出异常
                log.warn("Not authorized to read from topic {}.", tp.topic());
                throw new TopicAuthorizationException(Collections.singleton(tp.topic()));
            } else if (error == Errors.UNKNOWN) {
                log.warn("Unknown error fetching data for topic-partition {}", tp);
            } else {
                throw new IllegalStateException("Unexpected error code " + error.code() + " while fetching data");
            }
        } finally {
            completedFetch.metricAggregator.record(tp, bytes, recordsCount);
        }

        // we move the partition to the end if we received some bytes or if there was an error. This way, it's more
        // likely that partitions for the same topic can remain together (allowing for more efficient serialization).
        if (bytes > 0 || error != Errors.NONE)
            subscriptions.movePartitionToEnd(tp);
        // 返回封装好的PartitionRecords对象
        return parsedRecords;
    }

    /**
     * Parse the record entry, deserializing the key / value fields if necessary
     */
    private ConsumerRecord<K, V> parseRecord(TopicPartition partition, LogEntry logEntry) {
        // 得到消息记录
        Record record = logEntry.record();
        // 检查消息合法性
        if (this.checkCrcs) {
            try {
                record.ensureValid();
            } catch (InvalidRecordException e) {
                throw new KafkaException("Record for partition " + partition + " at offset " + logEntry.offset()
                        + " is invalid, cause: " + e.getMessage());
            }
        }

        try {
            // 得到消息偏移量和时间戳
            long offset = logEntry.offset();
            long timestamp = record.timestamp();
            TimestampType timestampType = record.timestampType();
            ByteBuffer keyBytes = record.key();
            // 反序列化得到消息的键
            byte[] keyByteArray = keyBytes == null ? null : Utils.toArray(keyBytes);
            K key = keyBytes == null ? null : this.keyDeserializer.deserialize(partition.topic(), keyByteArray);
            ByteBuffer valueBytes = record.value();
            // 反序列化得到消息的值
            byte[] valueByteArray = valueBytes == null ? null : Utils.toArray(valueBytes);
            V value = valueBytes == null ? null : this.valueDeserializer.deserialize(partition.topic(), valueByteArray);

            /**
             * 将上述信息包装为一个ConsumerRecord对象，该对象包含了以下信息：
             * 1. 主题
             * 2. 分区
             * 3. 偏移量
             * 4. 时间戳
             * 5. 时间戳类型
             * 6. 消息的校验码
             * 7. 消息的键长度
             * 8. 消息的值长度
             * 9. 消息的键
             * 10. 消息的值
             */
            return new ConsumerRecord<>(partition.topic(), partition.partition(), offset,
                                        timestamp, timestampType, record.checksum(),
                                        keyByteArray == null ? ConsumerRecord.NULL_SIZE : keyByteArray.length,
                                        valueByteArray == null ? ConsumerRecord.NULL_SIZE : valueByteArray.length,
                                        key, value);
        } catch (RuntimeException e) {
            throw new SerializationException("Error deserializing key/value for partition " + partition +
                    " at offset " + logEntry.offset(), e);
        }
    }

    @Override
    public void onAssignment(Set<TopicPartition> assignment) {
        sensors.updatePartitionLagSensors(assignment);
    }

    private static class PartitionRecords<K, V> {
        // records中第一条消息的offset
        private long fetchOffset;
        // 消息对应的TopicPartition
        private TopicPartition partition;
        // 消息集合
        private List<ConsumerRecord<K, V>> records;
        private int position = 0;

        private PartitionRecords(long fetchOffset, TopicPartition partition, List<ConsumerRecord<K, V>> records) {
            this.fetchOffset = fetchOffset;
            this.partition = partition;
            this.records = records;
        }

        private boolean isDrained() {
            return records == null;
        }

        private void drain() {
            this.records = null;
        }

        private List<ConsumerRecord<K, V>> drainRecords(int n) {
            if (isDrained() || position >= records.size()) {
                drain();
                return Collections.emptyList();
            }

            // using a sublist avoids a potentially expensive list copy (depending on the size of the records
            // and the maximum we can return from poll). The cost is that we cannot mutate the returned sublist.
            int limit = Math.min(records.size(), position + n);
            List<ConsumerRecord<K, V>> res = Collections.unmodifiableList(records.subList(position, limit));

            position = limit;
            if (position < records.size())
                fetchOffset = records.get(position).offset();

            return res;
        }
    }

    private static class CompletedFetch {
        // 分区
        private final TopicPartition partition;
        // 拉取的offset
        private final long fetchedOffset;
        // 拉取分区的数据
        private final FetchResponse.PartitionData partitionData;
        private final FetchResponseMetricAggregator metricAggregator;
        private final short responseVersion;

        private CompletedFetch(TopicPartition partition,
                               long fetchedOffset,
                               FetchResponse.PartitionData partitionData,
                               FetchResponseMetricAggregator metricAggregator,
                               short responseVersion) {
            this.partition = partition;
            this.fetchedOffset = fetchedOffset;
            this.partitionData = partitionData;
            this.metricAggregator = metricAggregator;
            this.responseVersion = responseVersion;
        }
    }

    /**
     * Since we parse the message data for each partition from each fetch response lazily, fetch-level
     * metrics need to be aggregated as the messages from each partition are parsed. This class is used
     * to facilitate this incremental aggregation.
     */
    private static class FetchResponseMetricAggregator {
        private final FetchManagerMetrics sensors;
        private final Set<TopicPartition> unrecordedPartitions;

        private final FetchMetrics fetchMetrics = new FetchMetrics();
        private final Map<String, FetchMetrics> topicFetchMetrics = new HashMap<>();

        private FetchResponseMetricAggregator(FetchManagerMetrics sensors,
                                              Set<TopicPartition> partitions) {
            this.sensors = sensors;
            this.unrecordedPartitions = partitions;
        }

        /**
         * After each partition is parsed, we update the current metric totals with the total bytes
         * and number of records parsed. After all partitions have reported, we write the metric.
         */
        public void record(TopicPartition partition, int bytes, int records) {
            this.unrecordedPartitions.remove(partition);
            this.fetchMetrics.increment(bytes, records);

            // collect and aggregate per-topic metrics
            String topic = partition.topic();
            FetchMetrics topicFetchMetric = this.topicFetchMetrics.get(topic);
            if (topicFetchMetric == null) {
                topicFetchMetric = new FetchMetrics();
                this.topicFetchMetrics.put(topic, topicFetchMetric);
            }
            topicFetchMetric.increment(bytes, records);

            if (this.unrecordedPartitions.isEmpty()) {
                // once all expected partitions from the fetch have reported in, record the metrics
                this.sensors.bytesFetched.record(topicFetchMetric.fetchBytes);
                this.sensors.recordsFetched.record(topicFetchMetric.fetchRecords);

                // also record per-topic metrics
                for (Map.Entry<String, FetchMetrics> entry: this.topicFetchMetrics.entrySet()) {
                    FetchMetrics metric = entry.getValue();
                    this.sensors.recordTopicFetchMetrics(entry.getKey(), metric.fetchBytes, metric.fetchRecords);
                }
            }
        }

        private static class FetchMetrics {
            private int fetchBytes;
            private int fetchRecords;

            protected void increment(int bytes, int records) {
                this.fetchBytes += bytes;
                this.fetchRecords += records;
            }
        }
    }

    private static class FetchManagerMetrics {
        private final Metrics metrics;
        private final String metricGrpName;
        private final Sensor bytesFetched;
        private final Sensor recordsFetched;
        private final Sensor fetchLatency;
        private final Sensor recordsFetchLag;
        private final Sensor fetchThrottleTimeSensor;

        private Set<TopicPartition> assignedPartitions;

        private FetchManagerMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metrics = metrics;
            this.metricGrpName = metricGrpPrefix + "-fetch-manager-metrics";

            this.bytesFetched = metrics.sensor("bytes-fetched");
            this.bytesFetched.add(metrics.metricName("fetch-size-avg",
                this.metricGrpName,
                "The average number of bytes fetched per request"), new Avg());
            this.bytesFetched.add(metrics.metricName("fetch-size-max",
                this.metricGrpName,
                "The maximum number of bytes fetched per request"), new Max());
            this.bytesFetched.add(metrics.metricName("bytes-consumed-rate",
                this.metricGrpName,
                "The average number of bytes consumed per second"), new Rate());

            this.recordsFetched = metrics.sensor("records-fetched");
            this.recordsFetched.add(metrics.metricName("records-per-request-avg",
                this.metricGrpName,
                "The average number of records in each request"), new Avg());
            this.recordsFetched.add(metrics.metricName("records-consumed-rate",
                this.metricGrpName,
                "The average number of records consumed per second"), new Rate());

            this.fetchLatency = metrics.sensor("fetch-latency");
            this.fetchLatency.add(metrics.metricName("fetch-latency-avg",
                this.metricGrpName,
                "The average time taken for a fetch request."), new Avg());
            this.fetchLatency.add(metrics.metricName("fetch-latency-max",
                this.metricGrpName,
                "The max time taken for any fetch request."), new Max());
            this.fetchLatency.add(metrics.metricName("fetch-rate",
                this.metricGrpName,
                "The number of fetch requests per second."), new Rate(new Count()));

            this.recordsFetchLag = metrics.sensor("records-lag");
            this.recordsFetchLag.add(metrics.metricName("records-lag-max",
                this.metricGrpName,
                "The maximum lag in terms of number of records for any partition in this window"), new Max());

            this.fetchThrottleTimeSensor = metrics.sensor("fetch-throttle-time");
            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-avg",
                                                         this.metricGrpName,
                                                         "The average throttle time in ms"), new Avg());

            this.fetchThrottleTimeSensor.add(metrics.metricName("fetch-throttle-time-max",
                                                         this.metricGrpName,
                                                         "The maximum throttle time in ms"), new Max());
        }

        private void recordTopicFetchMetrics(String topic, int bytes, int records) {
            // record bytes fetched
            String name = "topic." + topic + ".bytes-fetched";
            Sensor bytesFetched = this.metrics.getSensor(name);
            if (bytesFetched == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic.replace('.', '_'));

                bytesFetched = this.metrics.sensor(name);
                bytesFetched.add(this.metrics.metricName("fetch-size-avg",
                        this.metricGrpName,
                        "The average number of bytes fetched per request for topic " + topic,
                        metricTags), new Avg());
                bytesFetched.add(this.metrics.metricName("fetch-size-max",
                        this.metricGrpName,
                        "The maximum number of bytes fetched per request for topic " + topic,
                        metricTags), new Max());
                bytesFetched.add(this.metrics.metricName("bytes-consumed-rate",
                        this.metricGrpName,
                        "The average number of bytes consumed per second for topic " + topic,
                        metricTags), new Rate());
            }
            bytesFetched.record(bytes);

            // record records fetched
            name = "topic." + topic + ".records-fetched";
            Sensor recordsFetched = this.metrics.getSensor(name);
            if (recordsFetched == null) {
                Map<String, String> metricTags = new HashMap<>(1);
                metricTags.put("topic", topic.replace('.', '_'));

                recordsFetched = this.metrics.sensor(name);
                recordsFetched.add(this.metrics.metricName("records-per-request-avg",
                        this.metricGrpName,
                        "The average number of records in each request for topic " + topic,
                        metricTags), new Avg());
                recordsFetched.add(this.metrics.metricName("records-consumed-rate",
                        this.metricGrpName,
                        "The average number of records consumed per second for topic " + topic,
                        metricTags), new Rate());
            }
            recordsFetched.record(records);
        }

        private void updatePartitionLagSensors(Set<TopicPartition> assignedPartitions) {
            if (this.assignedPartitions != null) {
                for (TopicPartition tp : this.assignedPartitions) {
                    if (!assignedPartitions.contains(tp))
                        metrics.removeSensor(partitionLagMetricName(tp));
                }
            }
            this.assignedPartitions = assignedPartitions;
        }

        private void recordPartitionLag(TopicPartition tp, long lag) {
            this.recordsFetchLag.record(lag);

            String name = partitionLagMetricName(tp);
            Sensor recordsLag = this.metrics.getSensor(name);
            if (recordsLag == null) {
                recordsLag = this.metrics.sensor(name);
                recordsLag.add(this.metrics.metricName(name, this.metricGrpName, "The latest lag of the partition"),
                               new Value());
                recordsLag.add(this.metrics.metricName(name + "-max",
                        this.metricGrpName,
                        "The max lag of the partition"), new Max());
                recordsLag.add(this.metrics.metricName(name + "-avg",
                        this.metricGrpName,
                        "The average lag of the partition"), new Avg());
            }
            recordsLag.record(lag);
        }

        private static String partitionLagMetricName(TopicPartition tp) {
            return tp + ".records-lag";
        }
    }

}
