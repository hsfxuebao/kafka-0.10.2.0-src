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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 */

// 这个类被 client 线程和后台 sender 所共享,它只保存了所有 topic 的部分数据,当我们请求一个它上面没有的 topic meta 时,
// 它会通过发送 metadata update 来更新 meta 信息,
// 如果 topic meta 过期策略是允许的,那么任何 topic 过期的话都会被从集合中移除,
// 但是 consumer 是不允许 topic 过期的因为它明确地知道它需要管理哪些 topic
public final class Metadata {

    private static final Logger log = LoggerFactory.getLogger(Metadata.class);

    public static final long TOPIC_EXPIRY_MS = 5 * 60 * 1000;
    private static final long TOPIC_EXPIRY_NEEDS_UPDATE = -1L;
    //两个更新元数据的请求的最小的时间间隔，默认值是100ms
    //目的就是减少网络的压力
    private final long refreshBackoffMs;
    // //多久自动更新一次元数据，默认值是5分钟更新一次。
    private final long metadataExpireMs;
    // 集群元数据版本号，元数据更新成功一次，版本号就自增1
    private int version;
    // 上一次更新元数据的时间戳
    private long lastRefreshMs;
    /**
     * 上一次成功更新元数据的时间戳，如果每次更新都成功，
     * lastSuccessfulRefreshMs应该与lastRefreshMs相同，否则lastRefreshMs > lastSuccessfulRefreshMs
     */
    private long lastSuccessfulRefreshMs;
    // 记录kafka集群的元数据
    private Cluster cluster;
    // 表示是否强制更新Cluster
    private boolean needUpdate;
    /* Topics with expiry time */
    // 记录当前已知的所有的主题
    private final Map<String, Long> topics;
    // 监听器集合，用于监听Metadata更新
    private final List<Listener> listeners;
    // 当接收到 metadata 更新时, ClusterResourceListeners的列表
    private final ClusterResourceListeners clusterResourceListeners;
    // 是否需要更新全部主题的元数据
    private boolean needMetadataForAllTopics;
    // 默认为 true, Producer 会定时移除过期的 topic,consumer 则不会移除
    private final boolean topicExpiryEnabled;

    /**
     * Create a metadata instance with reasonable defaults
     */
    public Metadata() {
        this(100L, 60 * 60 * 1000L);
    }

    public Metadata(long refreshBackoffMs, long metadataExpireMs) {
        this(refreshBackoffMs, metadataExpireMs, false, new ClusterResourceListeners());
    }

    /**
     * Create a new Metadata instance
     * @param refreshBackoffMs The minimum amount of time that must expire between metadata refreshes to avoid busy
     *        polling
     * @param metadataExpireMs The maximum amount of time that metadata can be retained without refresh
     * @param topicExpiryEnabled If true, enable expiry of unused topics
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs, long metadataExpireMs, boolean topicExpiryEnabled, ClusterResourceListeners clusterResourceListeners) {
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.topicExpiryEnabled = topicExpiryEnabled;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.version = 0;
        this.cluster = Cluster.empty();
        this.needUpdate = false;
        this.topics = new HashMap<>();
        this.listeners = new ArrayList<>();
        this.clusterResourceListeners = clusterResourceListeners;
        this.needMetadataForAllTopics = false;
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        return this.cluster;
    }

    /**
     * Add the topic to maintain in the metadata. If topic expiry is enabled, expiry time
     * will be reset on the next update.
     */
    public synchronized void add(String topic) {
        if (topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE) == null) {
            requestUpdateForNewTopics();
        }
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        /**
         * 元数据是否过期，判断条件：
         * 1. needUpdate被置为true
         * 2. 上次更新时间距离当前时间已经超过了指定的元数据过期时间阈值metadataExpireMs（metadata.max.age.ms），默认是300秒
         */
        long timeToExpire = needUpdate ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        /**
         * 允许更新的时间点，计算方式：
         * 上次更新时间 + 退避时间 - 当前时间的间隔
         * 即要求上次更新时间与当前时间的间隔不能大于退避时间，如果大于则需要等待
         */
        long timeToAllowUpdate = this.lastRefreshMs + this.refreshBackoffMs - nowMs;
        return Math.max(timeToExpire, timeToAllowUpdate);
    }

    /**
     * Request an update of the current cluster metadata info, return the current version before the update
     */
    public synchronized int requestUpdate() {
        this.needUpdate = true; // 设置为需要强制更新
        return this.version; // 返回当前集群元数据的版本号
    }

    /**
     * Check whether an update has been explicitly requested.
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needUpdate;
    }

    /**
     * Wait for metadata update until the current version is larger than the last version we know of
     */
    public synchronized void awaitUpdate(final int lastVersion, final long maxWaitMs) throws InterruptedException {
        if (maxWaitMs < 0) {
            throw new IllegalArgumentException("Max time to wait for metadata updates should not be < 0 milli seconds");
        }
        long begin = System.currentTimeMillis();
        //看剩余可以使用的时间，一开始是最大等待的时间。
        long remainingWaitMs = maxWaitMs;
        //version是元数据的版本号。
        //如果当前的这个version小于等于上一次的version。
        //说明元数据还没更新。
        //因为如果sender线程那儿 更新元数据，如果更新成功了，sender线程肯定回去累加这个version。
        while (this.version <= lastVersion) {
            //如果还有剩余的时间。
            if (remainingWaitMs != 0)
                //让当前线程阻塞等待。
                //我们这儿虽然没有去看 sender线程的源码
                //但是我们知道，他那儿肯定会做这样的一个操作
                //如果更新元数据成功了，会唤醒这个线程。
                wait(remainingWaitMs);
            //如果代码执行到这儿 说明就要么就被唤醒了，要么就到点了。
            //计算一下花了多少时间。
            long elapsed = System.currentTimeMillis() - begin;
            // 超时，抛出超时异常
            if (elapsed >= maxWaitMs)
                throw new TimeoutException("Failed to update metadata after " + maxWaitMs + " ms.");
            //再次计算 可以使用的时间。
            remainingWaitMs = maxWaitMs - elapsed;
        }
    }

    /**
     * Replace the current set of topics maintained to the one provided.
     * If topic expiry is enabled, expiry time of the topics will be
     * reset on the next update.
     * @param topics
     */
    public synchronized void setTopics(Collection<String> topics) {
        // 如果Metadata中已知的主题没有包含传入的主题，则需要更新Metadata元数据
        if (!this.topics.keySet().containsAll(topics)) {
            requestUpdateForNewTopics();
        }
        // 清空原有的已知主题
        this.topics.clear();
        for (String topic : topics)
            // 更新已知主题
            this.topics.put(topic, TOPIC_EXPIRY_NEEDS_UPDATE);
    }

    /**
     * Get the list of topics we are currently maintaining metadata for
     */
    public synchronized Set<String> topics() {
        return new HashSet<>(this.topics.keySet());
    }

    /**
     * Check if a topic is already in the topic set.
     * @param topic topic to check
     * @return true if the topic exists, false otherwise
     */
    public synchronized boolean containsTopic(String topic) {
        return this.topics.containsKey(topic);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     */
    public synchronized void update(Cluster cluster, long now) {
        Objects.requireNonNull(cluster, "cluster should not be null");

        this.needUpdate = false;
        this.lastRefreshMs = now;
        this.lastSuccessfulRefreshMs = now;
        // 元数据版本 +1
        this.version += 1;
        // 默认是true
        if (topicExpiryEnabled) {
            // Handle expiry of topics from the metadata refresh set.
            // 场景驱动方式研究代码，第一次进来是在product初始化的时候
            // 但是我们目前topics是空的
            // 所以下面的代码是不会被运行的。

            //到现在我们的代码是不是第二次进来了呀？
            //如果第二次进来，此时此刻进来，我们 producer.send(topics,)方法
            //要去拉取元数据  -》 sender -》 代码走到的这儿。
            //第二次进来的时候，topics其实不为空了，因为我们已经给它赋值了
            //所以这儿的代码是会继续运行的。
            for (Iterator<Map.Entry<String, Long>> it = topics.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, Long> entry = it.next();
                long expireMs = entry.getValue();
                if (expireMs == TOPIC_EXPIRY_NEEDS_UPDATE)
                    entry.setValue(now + TOPIC_EXPIRY_MS);
                else if (expireMs <= now) {
                    it.remove();
                    log.debug("Removing unused topic {} from the metadata list, expiryMs {} now {}", entry.getKey(), expireMs, now);
                }
            }
        }
        // 通知所有的监听器，数据要更新了
        for (Listener listener: listeners)
            listener.onMetadataUpdate(cluster);

        String previousClusterId = cluster.clusterResource().clusterId();
        //这个的默认值是false，所以这个分支的代码不会被运行。
        if (this.needMetadataForAllTopics) {
            // the listener may change the interested topics, which could cause another metadata refresh.
            // If we have already fetched all topics, however, another fetch should be unnecessary.
            this.needUpdate = false;
            this.cluster = getClusterForCurrentTopics(cluster);
        } else {
            //所以代码执行的是这儿。
            //直接把刚刚传进来的对象赋值给了这个cluster。
            //cluster代表的是kafka集群的元数据。
            //初始化的时候，update这个方法没有去服务端拉取数据。
            this.cluster = cluster;
        }

        // The bootstrap cluster is guaranteed not to have any useful information
        if (!cluster.isBootstrapConfigured()) {
            String clusterId = cluster.clusterResource().clusterId();
            if (clusterId == null ? previousClusterId != null : !clusterId.equals(previousClusterId))
                log.info("Cluster ID: {}", cluster.clusterResource().clusterId());
            clusterResourceListeners.onUpdate(cluster.clusterResource());
        }
        //大家发现这儿会有一个notifyAll，这个最重要的一个作用是不是就是唤醒，我们上一讲
        //看到那个wait的线程。
        notifyAll();
        log.debug("Updated cluster metadata version {} to {}", this.version, this.cluster);
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        // 更新失败的情况下，只会更新lastRefreshMs字段
        this.lastRefreshMs = now;
    }

    /**
     * @return The current metadata version
     */
    public synchronized int version() {
        return this.version;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Set state to indicate if metadata for all topics in Kafka cluster is required or not.
     * @param needMetadataForAllTopics boolean indicating need for metadata of all topics in cluster.
     */
    public synchronized void needMetadataForAllTopics(boolean needMetadataForAllTopics) {
        if (needMetadataForAllTopics && !this.needMetadataForAllTopics) {
            requestUpdateForNewTopics();
        }
        this.needMetadataForAllTopics = needMetadataForAllTopics;
    }

    /**
     * Get whether metadata for all topics is needed or not
     */
    public synchronized boolean needMetadataForAllTopics() {
        return this.needMetadataForAllTopics;
    }

    /**
     * Add a Metadata listener that gets notified of metadata updates
     */
    public synchronized void addListener(Listener listener) {
        this.listeners.add(listener);
    }

    /**
     * Stop notifying the listener of metadata updates
     */
    public synchronized void removeListener(Listener listener) {
        this.listeners.remove(listener);
    }

    /**
     * MetadataUpdate Listener
     */
    public interface Listener {
        void onMetadataUpdate(Cluster cluster);
    }

    private synchronized void requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        this.lastRefreshMs = 0;
        requestUpdate();
    }
    // 根据传入的Cluster对象更新数据
    private Cluster getClusterForCurrentTopics(Cluster cluster) {
        Set<String> unauthorizedTopics = new HashSet<>();
        Collection<PartitionInfo> partitionInfos = new ArrayList<>();
        List<Node> nodes = Collections.emptyList();
        Set<String> internalTopics = Collections.emptySet();
        String clusterId = null;
        if (cluster != null) {

            clusterId = cluster.clusterResource().clusterId();
            internalTopics = cluster.internalTopics();
            // 记录未授权的主题
            unauthorizedTopics.addAll(cluster.unauthorizedTopics());
            // 从未授权的主题中移除当前已知可用的主题
            unauthorizedTopics.retainAll(this.topics.keySet());
            // 更新partition信息
            for (String topic : this.topics.keySet()) {
                List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
                if (partitionInfoList != null) {
                    partitionInfos.addAll(partitionInfoList);
                }
            }
            nodes = cluster.nodes();
        }
        // 构造新的Cluster
        return new Cluster(clusterId, nodes, partitionInfos, unauthorizedTopics, internalTopics);
    }
}
