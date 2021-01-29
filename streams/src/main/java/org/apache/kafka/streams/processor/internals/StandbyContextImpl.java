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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetrics;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.internals.ThreadCache;
import java.util.Collections;
import java.util.Map;

class StandbyContextImpl extends AbstractProcessorContext implements RecordCollector.Supplier {

    private static final RecordCollector NO_OP_COLLECTOR = new RecordCollector() {
        @Override
        public <K, V> void send(final String topic,
                                K key,
                                V value,
                                Integer partition,
                                Long timestamp,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer) {
        }

        @Override
        public <K, V> void send(final String topic,
                                K key,
                                V value,
                                Integer partition,
                                Long timestamp,
                                Serializer<K> keySerializer,
                                Serializer<V> valueSerializer,
                                StreamPartitioner<? super K, ? super V> partitioner) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }

        @Override
        public Map<TopicPartition, Long> offsets() {
            return Collections.emptyMap();
        }
    };

    public StandbyContextImpl(final TaskId id,
                       final String applicationId,
                       final StreamsConfig config,
                       final ProcessorStateManager stateMgr,
                       final StreamsMetrics metrics) {
        super(id, applicationId, config, metrics, stateMgr, new ThreadCache("zeroCache", 0, metrics));
    }


    StateManager getStateMgr() {
        return stateManager;
    }

    @Override
    public RecordCollector recordCollector() {
        return NO_OP_COLLECTOR;
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public StateStore getStateStore(String name) {
        throw new UnsupportedOperationException("this should not happen: getStateStore() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public String topic() {
        throw new UnsupportedOperationException("this should not happen: topic() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public int partition() {
        throw new UnsupportedOperationException("this should not happen: partition() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public long offset() {
        throw new UnsupportedOperationException("this should not happen: offset() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public long timestamp() {
        throw new UnsupportedOperationException("this should not happen: timestamp() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public <K, V> void forward(K key, V value) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public <K, V> void forward(K key, V value, int childIndex) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public <K, V> void forward(K key, V value, String childName) {
        throw new UnsupportedOperationException("this should not happen: forward() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void commit() {
        throw new UnsupportedOperationException("this should not happen: commit() not supported in standby tasks.");
    }

    /**
     * @throws UnsupportedOperationException
     */
    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException("this should not happen: schedule() not supported in standby tasks.");
    }


    @Override
    public RecordContext recordContext() {
        throw new UnsupportedOperationException("this should not happen: recordContext not supported in standby tasks.");
    }

    @Override
    public void setRecordContext(final RecordContext recordContext) {
        throw new UnsupportedOperationException("this should not happen: setRecordContext not supported in standby tasks.");
    }


    @Override
    public void setCurrentNode(final ProcessorNode currentNode) {
        // no-op. can't throw as this is called on commit when the StateStores get flushed.
    }

    @Override
    public ProcessorNode currentNode() {
        throw new UnsupportedOperationException("this should not happen: currentNode not supported in standby tasks.");
    }
}
