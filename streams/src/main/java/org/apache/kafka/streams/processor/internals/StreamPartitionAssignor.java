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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.errors.TopologyBuilderException;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.processor.internals.assignment.AssignmentInfo;
import org.apache.kafka.streams.processor.internals.assignment.ClientState;
import org.apache.kafka.streams.processor.internals.assignment.SubscriptionInfo;
import org.apache.kafka.streams.processor.internals.assignment.TaskAssignor;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.kafka.common.utils.Utils.getHost;
import static org.apache.kafka.common.utils.Utils.getPort;
import static org.apache.kafka.streams.processor.internals.InternalTopicManager.WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT;

public class StreamPartitionAssignor implements PartitionAssignor, Configurable {

    private static final Logger log = LoggerFactory.getLogger(StreamPartitionAssignor.class);

    private final static int UNKNOWN = -1;
    public final static int NOT_AVAILABLE = -2;

    private static class AssignedPartition implements Comparable<AssignedPartition> {
        public final TaskId taskId;
        public final TopicPartition partition;

        AssignedPartition(final TaskId taskId, final TopicPartition partition) {
            this.taskId = taskId;
            this.partition = partition;
        }

        @Override
        public int compareTo(final AssignedPartition that) {
            return PARTITION_COMPARATOR.compare(this.partition, that.partition);
        }
    }

    private static class ClientMetadata {
        final HostInfo hostInfo;
        final Set<String> consumers;
        final ClientState<TaskId> state;

        ClientMetadata(final String endPoint) {

            // get the host info if possible
            if (endPoint != null) {
                final String host = getHost(endPoint);
                final Integer port = getPort(endPoint);

                if (host == null || port == null)
                    throw new ConfigException(String.format("Error parsing host address %s. Expected format host:port.", endPoint));

                hostInfo = new HostInfo(host, port);
            } else {
                hostInfo = null;
            }

            // initialize the consumer memberIds
            consumers = new HashSet<>();

            // initialize the client state
            state = new ClientState<>();
        }

        void addConsumer(final String consumerMemberId, final SubscriptionInfo info) {

            consumers.add(consumerMemberId);

            state.prevActiveTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.prevTasks);
            state.prevAssignedTasks.addAll(info.standbyTasks);
            state.capacity = state.capacity + 1d;
        }

        @Override
        public String toString() {
            return "ClientMetadata{" +
                    "hostInfo=" + hostInfo +
                    ", consumers=" + consumers +
                    ", state=" + state +
                    '}';
        }
    }

    static class InternalTopicMetadata {
        public final InternalTopicConfig config;

        public int numPartitions;

        InternalTopicMetadata(final InternalTopicConfig config) {
            this.config = config;
            this.numPartitions = UNKNOWN;
        }
    }

    private static final Comparator<TopicPartition> PARTITION_COMPARATOR = new Comparator<TopicPartition>() {
        @Override
        public int compare(TopicPartition p1, TopicPartition p2) {
            int result = p1.topic().compareTo(p2.topic());

            if (result != 0) {
                return result;
            } else {
                return p1.partition() < p2.partition() ? UNKNOWN : (p1.partition() > p2.partition() ? 1 : 0);
            }
        }
    };

    private StreamThread streamThread;

    private String userEndPoint;
    private int numStandbyReplicas;

    private Cluster metadataWithInternalTopics;
    private Map<HostInfo, Set<TopicPartition>> partitionsByHostState;

    private Map<TaskId, Set<TopicPartition>> standbyTasks;
    private Map<TaskId, Set<TopicPartition>> activeTasks;

    private InternalTopicManager internalTopicManager;
    private CopartitionedTopicsValidator copartitionedTopicsValidator;

    /**
     * We need to have the PartitionAssignor and its StreamThread to be mutually accessible
     * since the former needs later's cached metadata while sending subscriptions,
     * and the latter needs former's returned assignment when adding tasks.
     * @throws KafkaException if the stream thread is not specified
     */
    @Override
    public void configure(Map<String, ?> configs) {
        numStandbyReplicas = (Integer) configs.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG);

        Object o = configs.get(StreamsConfig.InternalConfig.STREAM_THREAD_INSTANCE);
        if (o == null) {
            KafkaException ex = new KafkaException("StreamThread is not specified");
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        if (!(o instanceof StreamThread)) {
            KafkaException ex = new KafkaException(String.format("%s is not an instance of %s", o.getClass().getName(), StreamThread.class.getName()));
            log.error(ex.getMessage(), ex);
            throw ex;
        }

        streamThread = (StreamThread) o;
        streamThread.partitionAssignor(this);

        String userEndPoint = (String) configs.get(StreamsConfig.APPLICATION_SERVER_CONFIG);
        if (userEndPoint != null && !userEndPoint.isEmpty()) {
            try {
                String host = getHost(userEndPoint);
                Integer port = getPort(userEndPoint);

                if (host == null || port == null)
                    throw new ConfigException(String.format("stream-thread [%s] Config %s isn't in the correct format. Expected a host:port pair" +
                                    " but received %s",
                            streamThread.getName(), StreamsConfig.APPLICATION_SERVER_CONFIG, userEndPoint));
            } catch (NumberFormatException nfe) {
                throw new ConfigException(String.format("stream-thread [%s] Invalid port supplied in %s for config %s",
                        streamThread.getName(), userEndPoint, StreamsConfig.APPLICATION_SERVER_CONFIG));
            }

            this.userEndPoint = userEndPoint;
        }

        internalTopicManager = new InternalTopicManager(
                new StreamsKafkaClient(this.streamThread.config),
                configs.containsKey(StreamsConfig.REPLICATION_FACTOR_CONFIG) ? (Integer) configs.get(StreamsConfig.REPLICATION_FACTOR_CONFIG) : 1,
                configs.containsKey(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG) ?
                        (Long) configs.get(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG)
                        : WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_DEFAULT);

        this.copartitionedTopicsValidator = new CopartitionedTopicsValidator(streamThread.getName());
    }

    @Override
    public String name() {
        return "stream";
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        // Adds the following information to subscription
        // 1. Client UUID (a unique id assigned to an instance of KafkaStreams)
        // 2. Task ids of previously running tasks
        // 3. Task ids of valid local states on the client's state directory.

        Set<TaskId> prevTasks = streamThread.prevTasks();
        Set<TaskId> standbyTasks = streamThread.cachedTasks();
        standbyTasks.removeAll(prevTasks);
        SubscriptionInfo data = new SubscriptionInfo(streamThread.processId, prevTasks, standbyTasks, this.userEndPoint);

        if (streamThread.builder.sourceTopicPattern() != null) {
            SubscriptionUpdates subscriptionUpdates = new SubscriptionUpdates();
            log.debug("stream-thread [{}] found {} topics possibly matching regex", streamThread.getName(), topics);
            // update the topic groups with the returned subscription set for regex pattern subscriptions
            subscriptionUpdates.updateTopics(topics);
            streamThread.builder.updateSubscriptions(subscriptionUpdates, streamThread.getName());
        }

        return new Subscription(new ArrayList<>(topics), data.encode());
    }

    /*
     * This assigns tasks to consumer clients in the following steps.
     *
     * 0. check all repartition source topics and use internal topic manager to make sure
     *    they have been created with the right number of partitions.
     *
     * 1. using user customized partition grouper to generate tasks along with their
     *    assigned partitions; also make sure that the task's corresponding changelog topics
     *    have been created with the right number of partitions.
     *
     * 2. using TaskAssignor to assign tasks to consumer clients.
     *    - Assign a task to a client which was running it previously.
     *      If there is no such client, assign a task to a client which has its valid local state.
     *    - A client may have more than one stream threads.
     *      The assignor tries to assign tasks to a client proportionally to the number of threads.
     *    - We try not to assign the same set of tasks to two different clients
     *    We do the assignment in one-pass. The result may not satisfy above all.
     *
     * 3. within each client, tasks are assigned to consumer clients in round-robin manner.
     */
    @Override
    public Map<String, Assignment> assign(Cluster metadata, Map<String, Subscription> subscriptions) {
        // construct the client metadata from the decoded subscription info
        Map<UUID, ClientMetadata> clientsMetadata = new HashMap<>();

        for (Map.Entry<String, Subscription> entry : subscriptions.entrySet()) {
            String consumerId = entry.getKey();
            Subscription subscription = entry.getValue();

            SubscriptionInfo info = SubscriptionInfo.decode(subscription.userData());

            // create the new client metadata if necessary
            ClientMetadata clientMetadata = clientsMetadata.get(info.processId);

            if (clientMetadata == null) {
                clientMetadata = new ClientMetadata(info.userEndPoint);
                clientsMetadata.put(info.processId, clientMetadata);
            }

            // add the consumer to the client
            clientMetadata.addConsumer(consumerId, info);
        }

        log.info("stream-thread [{}] Constructed client metadata {} from the member subscriptions.", streamThread.getName(), clientsMetadata);

        // ---------------- Step Zero ---------------- //

        // parse the topology to determine the repartition source topics,
        // making sure they are created with the number of partitions as
        // the maximum of the depending sub-topologies source topics' number of partitions
        Map<Integer, TopologyBuilder.TopicsInfo> topicGroups = streamThread.builder.topicGroups();

        Map<String, InternalTopicMetadata> repartitionTopicMetadata = new HashMap<>();
        for (TopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
            for (InternalTopicConfig topic: topicsInfo.repartitionSourceTopics.values()) {
                repartitionTopicMetadata.put(topic.name(), new InternalTopicMetadata(topic));
            }
        }

        boolean numPartitionsNeeded;
        do {
            numPartitionsNeeded = false;

            for (TopologyBuilder.TopicsInfo topicsInfo : topicGroups.values()) {
                for (String topicName : topicsInfo.repartitionSourceTopics.keySet()) {
                    int numPartitions = repartitionTopicMetadata.get(topicName).numPartitions;

                    // try set the number of partitions for this repartition topic if it is not set yet
                    if (numPartitions == UNKNOWN) {
                        for (TopologyBuilder.TopicsInfo otherTopicsInfo : topicGroups.values()) {
                            Set<String> otherSinkTopics = otherTopicsInfo.sinkTopics;

                            if (otherSinkTopics.contains(topicName)) {
                                // if this topic is one of the sink topics of this topology,
                                // use the maximum of all its source topic partitions as the number of partitions
                                for (String sourceTopicName : otherTopicsInfo.sourceTopics) {
                                    Integer numPartitionsCandidate;
                                    // It is possible the sourceTopic is another internal topic, i.e,
                                    // map().join().join(map())
                                    if (repartitionTopicMetadata.containsKey(sourceTopicName)) {
                                        numPartitionsCandidate = repartitionTopicMetadata.get(sourceTopicName).numPartitions;
                                    } else {
                                        numPartitionsCandidate = metadata.partitionCountForTopic(sourceTopicName);
                                        if (numPartitionsCandidate == null) {
                                            repartitionTopicMetadata.get(topicName).numPartitions = NOT_AVAILABLE;
                                        }
                                    }

                                    if (numPartitionsCandidate != null && numPartitionsCandidate > numPartitions) {
                                        numPartitions = numPartitionsCandidate;
                                    }
                                }
                            }
                        }
                        // if we still have not find the right number of partitions,
                        // another iteration is needed
                        if (numPartitions == UNKNOWN)
                            numPartitionsNeeded = true;
                        else
                            repartitionTopicMetadata.get(topicName).numPartitions = numPartitions;
                    }
                }
            }
        } while (numPartitionsNeeded);

        // augment the metadata with the newly computed number of partitions for all the
        // repartition source topics
        Map<TopicPartition, PartitionInfo> allRepartitionTopicPartitions = new HashMap<>();
        for (Map.Entry<String, InternalTopicMetadata> entry : repartitionTopicMetadata.entrySet()) {
            String topic = entry.getKey();
            Integer numPartitions = entry.getValue().numPartitions;

            for (int partition = 0; partition < numPartitions; partition++) {
                allRepartitionTopicPartitions.put(new TopicPartition(topic, partition),
                        new PartitionInfo(topic, partition, null, new Node[0], new Node[0]));
            }
        }

        // ensure the co-partitioning topics within the group have the same number of partitions,
        // and enforce the number of partitions for those repartition topics to be the same if they
        // are co-partitioned as well.
        ensureCopartitioning(streamThread.builder.copartitionGroups(), repartitionTopicMetadata, metadata);

        // make sure the repartition source topics exist with the right number of partitions,
        // create these topics if necessary
        prepareTopic(repartitionTopicMetadata);

        metadataWithInternalTopics = metadata.withPartitions(allRepartitionTopicPartitions);

        log.debug("stream-thread [{}] Created repartition topics {} from the parsed topology.", streamThread.getName(), allRepartitionTopicPartitions.values());

        // ---------------- Step One ---------------- //

        // get the tasks as partition groups from the partition grouper
        Set<String> allSourceTopics = new HashSet<>();
        Map<Integer, Set<String>> sourceTopicsByGroup = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            allSourceTopics.addAll(entry.getValue().sourceTopics);
            sourceTopicsByGroup.put(entry.getKey(), entry.getValue().sourceTopics);
        }

        Map<TaskId, Set<TopicPartition>> partitionsForTask = streamThread.partitionGrouper.partitionGroups(
                sourceTopicsByGroup, metadataWithInternalTopics);

        // check if all partitions are assigned, and there are no duplicates of partitions in multiple tasks
        Set<TopicPartition> allAssignedPartitions = new HashSet<>();
        Map<Integer, Set<TaskId>> tasksByTopicGroup = new HashMap<>();
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : partitionsForTask.entrySet()) {
            Set<TopicPartition> partitions = entry.getValue();
            for (TopicPartition partition : partitions) {
                if (allAssignedPartitions.contains(partition)) {
                    log.warn("stream-thread [{}] Partition {} is assigned to more than one tasks: {}", streamThread.getName(), partition, partitionsForTask);
                }
            }
            allAssignedPartitions.addAll(partitions);

            TaskId id = entry.getKey();
            Set<TaskId> ids = tasksByTopicGroup.get(id.topicGroupId);
            if (ids == null) {
                ids = new HashSet<>();
                tasksByTopicGroup.put(id.topicGroupId, ids);
            }
            ids.add(id);
        }
        for (String topic : allSourceTopics) {
            List<PartitionInfo> partitionInfoList = metadataWithInternalTopics.partitionsForTopic(topic);
            if (partitionInfoList != null) {
                for (PartitionInfo partitionInfo : partitionInfoList) {
                    TopicPartition partition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    if (!allAssignedPartitions.contains(partition)) {
                        log.warn("stream-thread [{}] Partition {} is not assigned to any tasks: {}", streamThread.getName(), partition, partitionsForTask);
                    }
                }
            } else {
                log.warn("stream-thread [{}] No partitions found for topic {}", streamThread.getName(), topic);
            }
        }

        // add tasks to state change log topic subscribers
        Map<String, InternalTopicMetadata> changelogTopicMetadata = new HashMap<>();
        for (Map.Entry<Integer, TopologyBuilder.TopicsInfo> entry : topicGroups.entrySet()) {
            final int topicGroupId = entry.getKey();
            final Map<String, InternalTopicConfig> stateChangelogTopics = entry.getValue().stateChangelogTopics;

            for (InternalTopicConfig topicConfig : stateChangelogTopics.values()) {
                // the expected number of partitions is the max value of TaskId.partition + 1
                int numPartitions = UNKNOWN;
                if (tasksByTopicGroup.get(topicGroupId) != null) {
                    for (TaskId task : tasksByTopicGroup.get(topicGroupId)) {
                        if (numPartitions < task.partition + 1)
                            numPartitions = task.partition + 1;
                    }
                    InternalTopicMetadata topicMetadata = new InternalTopicMetadata(topicConfig);
                    topicMetadata.numPartitions = numPartitions;

                    changelogTopicMetadata.put(topicConfig.name(), topicMetadata);
                } else {
                    log.debug("stream-thread [{}] No tasks found for topic group {}", streamThread.getName(), topicGroupId);
                }
            }
        }

        prepareTopic(changelogTopicMetadata);

        log.debug("stream-thread [{}] Created state changelog topics {} from the parsed topology.", streamThread.getName(), changelogTopicMetadata);

        // ---------------- Step Two ---------------- //

        // assign tasks to clients
        Map<UUID, ClientState<TaskId>> states = new HashMap<>();
        for (Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            states.put(entry.getKey(), entry.getValue().state);
        }

        log.debug("stream-thread [{}] Assigning tasks {} to clients {} with number of replicas {}",
                streamThread.getName(), partitionsForTask.keySet(), states, numStandbyReplicas);

        TaskAssignor.assign(states, partitionsForTask.keySet(), numStandbyReplicas);

        log.info("stream-thread [{}] Assigned tasks to clients as {}.", streamThread.getName(), states);

        // ---------------- Step Three ---------------- //

        // construct the global partition assignment per host map
        partitionsByHostState = new HashMap<>();
        for (Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            HostInfo hostInfo = entry.getValue().hostInfo;

            if (hostInfo != null) {
                final Set<TopicPartition> topicPartitions = new HashSet<>();
                final ClientState<TaskId> state = entry.getValue().state;

                for (TaskId id : state.activeTasks) {
                    topicPartitions.addAll(partitionsForTask.get(id));
                }

                partitionsByHostState.put(hostInfo, topicPartitions);
            }
        }

        // within the client, distribute tasks to its owned consumers
        Map<String, Assignment> assignment = new HashMap<>();
        for (Map.Entry<UUID, ClientMetadata> entry : clientsMetadata.entrySet()) {
            Set<String> consumers = entry.getValue().consumers;
            ClientState<TaskId> state = entry.getValue().state;

            ArrayList<TaskId> taskIds = new ArrayList<>(state.assignedTasks.size());
            final int numActiveTasks = state.activeTasks.size();

            taskIds.addAll(state.activeTasks);
            taskIds.addAll(state.standbyTasks);

            final int numConsumers = consumers.size();

            int i = 0;
            for (String consumer : consumers) {
                Map<TaskId, Set<TopicPartition>> standby = new HashMap<>();
                ArrayList<AssignedPartition> assignedPartitions = new ArrayList<>();

                final int numTaskIds = taskIds.size();
                for (int j = i; j < numTaskIds; j += numConsumers) {
                    TaskId taskId = taskIds.get(j);
                    if (j < numActiveTasks) {
                        for (TopicPartition partition : partitionsForTask.get(taskId)) {
                            assignedPartitions.add(new AssignedPartition(taskId, partition));
                        }
                    } else {
                        Set<TopicPartition> standbyPartitions = standby.get(taskId);
                        if (standbyPartitions == null) {
                            standbyPartitions = new HashSet<>();
                            standby.put(taskId, standbyPartitions);
                        }
                        standbyPartitions.addAll(partitionsForTask.get(taskId));
                    }
                }

                Collections.sort(assignedPartitions);
                List<TaskId> active = new ArrayList<>();
                List<TopicPartition> activePartitions = new ArrayList<>();
                for (AssignedPartition partition : assignedPartitions) {
                    active.add(partition.taskId);
                    activePartitions.add(partition.partition);
                }

                // finally, encode the assignment before sending back to coordinator
                assignment.put(consumer, new Assignment(activePartitions, new AssignmentInfo(active, standby, partitionsByHostState).encode()));
                i++;
            }
        }

        return assignment;
    }

    /**
     * @throws TaskAssignmentException if there is no task id for one of the partitions specified
     */
    @Override
    public void onAssignment(Assignment assignment) {
        List<TopicPartition> partitions = new ArrayList<>(assignment.partitions());
        Collections.sort(partitions, PARTITION_COMPARATOR);

        AssignmentInfo info = AssignmentInfo.decode(assignment.userData());

        this.standbyTasks = info.standbyTasks;
        this.activeTasks = new HashMap<>();

        // the number of assigned partitions should be the same as number of active tasks, which
        // could be duplicated if one task has more than one assigned partitions
        if (partitions.size() != info.activeTasks.size()) {
            throw new TaskAssignmentException(
                    String.format("stream-thread [%s] Number of assigned partitions %d is not equal to the number of active taskIds %d" +
                            ", assignmentInfo=%s", streamThread.getName(), partitions.size(), info.activeTasks.size(), info.toString())
            );
        }

        for (int i = 0; i < partitions.size(); i++) {
            TopicPartition partition = partitions.get(i);
            TaskId id = info.activeTasks.get(i);

            Set<TopicPartition> assignedPartitions = activeTasks.get(id);
            if (assignedPartitions == null) {
                assignedPartitions = new HashSet<>();
                activeTasks.put(id, assignedPartitions);
            }
            assignedPartitions.add(partition);
        }

        this.partitionsByHostState = info.partitionsByHost;

        final Collection<Set<TopicPartition>> values = partitionsByHostState.values();
        final Map<TopicPartition, PartitionInfo> topicToPartitionInfo = new HashMap<>();
        for (Set<TopicPartition> value : values) {
            for (TopicPartition topicPartition : value) {
                topicToPartitionInfo.put(topicPartition, new PartitionInfo(topicPartition.topic(),
                                                                           topicPartition.partition(),
                                                                           null,
                                                                           new Node[0],
                                                                           new Node[0]));
            }
        }
        metadataWithInternalTopics = Cluster.empty().withPartitions(topicToPartitionInfo);
    }

    /**
     * Internal helper function that creates a Kafka topic
     *
     * @param topicPartitions Map that contains the topic names to be created with the number of partitions
     */
    private void prepareTopic(final Map<String, InternalTopicMetadata> topicPartitions) {
        log.debug("stream-thread [{}] Starting to validate internal topics in partition assignor.", streamThread.getName());

        // first construct the topics to make ready
        Map<InternalTopicConfig, Integer> topicsToMakeReady = new HashMap<>();
        Set<String> topicNamesToMakeReady = new HashSet<>();

        for (InternalTopicMetadata metadata : topicPartitions.values()) {
            InternalTopicConfig topic = metadata.config;
            Integer numPartitions = metadata.numPartitions;

            if (numPartitions == NOT_AVAILABLE) {
                continue;
            }
            if (numPartitions < 0) {
                throw new TopologyBuilderException(String.format("stream-thread [%s] Topic [%s] number of partitions not defined", streamThread.getName(), topic.name()));
            }

            topicsToMakeReady.put(topic, numPartitions);
            topicNamesToMakeReady.add(topic.name());
        }

        if (!topicsToMakeReady.isEmpty()) {
            internalTopicManager.makeReady(topicsToMakeReady);

            // wait until each one of the topic metadata has been propagated to at least one broker
            while (!allTopicsCreated(topicNamesToMakeReady, topicsToMakeReady)) {
                try {
                    Thread.sleep(50L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }

        log.info("stream-thread [{}] Completed validating internal topics in partition assignor", streamThread.getName());
    }

    private boolean allTopicsCreated(final Set<String> topicNamesToMakeReady, final Map<InternalTopicConfig, Integer> topicsToMakeReady) {
        final Map<String, Integer> partitions = internalTopicManager.getNumPartitions(topicNamesToMakeReady);
        for (Map.Entry<InternalTopicConfig, Integer> entry : topicsToMakeReady.entrySet()) {
            final Integer numPartitions = partitions.get(entry.getKey().name());
            if (numPartitions == null || !numPartitions.equals(entry.getValue())) {
                return false;
            }
        }
        return true;
    }

    private void ensureCopartitioning(Collection<Set<String>> copartitionGroups,
                                      Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                                      Cluster metadata) {
        for (Set<String> copartitionGroup : copartitionGroups) {
            copartitionedTopicsValidator.validate(copartitionGroup, allRepartitionTopicsNumPartitions, metadata);
        }
    }

    Map<HostInfo, Set<TopicPartition>> getPartitionsByHostState() {
        if (partitionsByHostState == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(partitionsByHostState);
    }

    Cluster clusterMetadata() {
        if (metadataWithInternalTopics == null) {
            return Cluster.empty();
        }
        return metadataWithInternalTopics;
    }

    Map<TaskId, Set<TopicPartition>> activeTasks() {
        if (activeTasks == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(activeTasks);
    }

    Map<TaskId, Set<TopicPartition>> standbyTasks() {
        if (standbyTasks == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(standbyTasks);
    }

    void setInternalTopicManager(InternalTopicManager internalTopicManager) {
        this.internalTopicManager = internalTopicManager;
    }

    /**
     * Used to capture subscribed topic via Patterns discovered during the
     * partition assignment process.
     */
    public static class SubscriptionUpdates {

        private final Set<String> updatedTopicSubscriptions = new HashSet<>();

        private  void updateTopics(Collection<String> topicNames) {
            updatedTopicSubscriptions.clear();
            updatedTopicSubscriptions.addAll(topicNames);
        }

        public Collection<String> getUpdates() {
            return Collections.unmodifiableSet(new HashSet<>(updatedTopicSubscriptions));
        }

        public boolean hasUpdates() {
            return !updatedTopicSubscriptions.isEmpty();
        }

        @Override
        public String toString() {
            return "SubscriptionUpdates{" +
                    "updatedTopicSubscriptions=" + updatedTopicSubscriptions +
                    '}';
        }
    }

    public void close() {
        internalTopicManager.close();
    }

    static class CopartitionedTopicsValidator {
        private final String threadName;

        CopartitionedTopicsValidator(final String threadName) {
            this.threadName = threadName;
        }

        void validate(final Set<String> copartitionGroup,
                      final Map<String, InternalTopicMetadata> allRepartitionTopicsNumPartitions,
                      final Cluster metadata) {
            int numPartitions = UNKNOWN;

            for (final String topic : copartitionGroup) {
                if (!allRepartitionTopicsNumPartitions.containsKey(topic)) {
                    final Integer partitions = metadata.partitionCountForTopic(topic);

                    if (partitions == null) {
                        throw new TopologyBuilderException(String.format("stream-thread [%s] Topic not found: %s", threadName, topic));
                    }

                    if (numPartitions == UNKNOWN) {
                        numPartitions = partitions;
                    } else if (numPartitions != partitions) {
                        final String[] topics = copartitionGroup.toArray(new String[copartitionGroup.size()]);
                        Arrays.sort(topics);
                        throw new TopologyBuilderException(String.format("stream-thread [%s] Topics not co-partitioned: [%s]", threadName, Utils.mkString(Arrays.asList(topics), ",")));
                    }
                } else if (allRepartitionTopicsNumPartitions.get(topic).numPartitions == NOT_AVAILABLE) {
                    numPartitions = NOT_AVAILABLE;
                    break;
                }
            }

            // if all topics for this co-partition group is repartition topics,
            // then set the number of partitions to be the maximum of the number of partitions.
            if (numPartitions == UNKNOWN) {
                for (Map.Entry<String, InternalTopicMetadata> entry: allRepartitionTopicsNumPartitions.entrySet()) {
                    if (copartitionGroup.contains(entry.getKey())) {
                        final int partitions = entry.getValue().numPartitions;
                        if (partitions > numPartitions) {
                            numPartitions = partitions;
                        }
                    }
                }
            }
            // enforce co-partitioning restrictions to repartition topics by updating their number of partitions
            for (Map.Entry<String, InternalTopicMetadata> entry : allRepartitionTopicsNumPartitions.entrySet()) {
                if (copartitionGroup.contains(entry.getKey())) {
                    entry.getValue().numPartitions = numPartitions;
                }
            }

        }
    }
}
