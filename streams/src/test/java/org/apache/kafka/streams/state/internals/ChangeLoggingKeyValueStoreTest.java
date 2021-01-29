/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.test.InMemoryKeyValueStore;
import org.apache.kafka.test.MockProcessorContext;
import org.apache.kafka.test.NoOpRecordCollector;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;

public class ChangeLoggingKeyValueStoreTest {

    private final InMemoryKeyValueStore<Bytes, byte[]> inner = new InMemoryKeyValueStore<>("kv");
    private final Serde<String> keySerde = Serdes.String();
    private final Serde<String> valueSerde = Serdes.String();
    private final ChangeLoggingKeyValueStore<String, String> store
            = new ChangeLoggingKeyValueStore<>(inner, keySerde, valueSerde);
    private final Map sent = new HashMap<>();
    private final String hi = "hi";
    private final Bytes hiBytes = Bytes.wrap(hi.getBytes());
    private final String there = "there";
    private final byte[] thereBytes = "there".getBytes();
    private final String hello = "hello";
    private final String world = "world";

    @Before
    public void before() {
        final NoOpRecordCollector collector = new NoOpRecordCollector() {
            @Override
            public <K, V> void send(final String topic,
                                    K key,
                                    V value,
                                    Integer partition,
                                    Long timestamp,
                                    Serializer<K> keySerializer,
                                    Serializer<V> valueSerializer) {
                sent.put(key, value);
            }
        };
        final MockProcessorContext context = new MockProcessorContext(TestUtils.tempDirectory(),
                                                                      Serdes.String(),
                                                                      Serdes.Long(),
                                                                      collector,
                                                                      new ThreadCache("testCache", 0, new MockStreamsMetrics(new Metrics())));
        context.setTime(0);
        store.init(context, store);
    }

    @Test
    public void shouldWriteKeyValueBytesToInnerStoreOnPut() throws Exception {
        store.put(hi, there);
        assertThat(deserializedValueFromInner(hi), equalTo(there));
    }

    @Test
    public void shouldLogChangeOnPut() throws Exception {
        store.put(hi, there);
        assertThat((byte[]) sent.get(hiBytes), equalTo(thereBytes));
    }

    @Test
    public void shouldWriteAllKeyValueToInnerStoreOnPutAll() throws Exception {
        store.putAll(Arrays.asList(KeyValue.pair(hello, world),
                                   KeyValue.pair(hi, there)));
        assertThat(deserializedValueFromInner(hello), equalTo(world));
        assertThat(deserializedValueFromInner(hi), equalTo(there));
    }

    @Test
    public void shouldLogChangesOnPutAll() throws Exception {
        store.putAll(Arrays.asList(KeyValue.pair(hi, there),
                                   KeyValue.pair(hello, world)));
        assertThat((byte[]) sent.get(hiBytes), equalTo(thereBytes));
        assertThat((byte[]) sent.get(Bytes.wrap(hello.getBytes())), equalTo(world.getBytes()));
    }

    @Test
    public void shouldPutNullOnDelete() throws Exception {
        store.put(hi, there);
        store.delete(hi);
        assertThat(inner.get(hiBytes), nullValue());
    }

    @Test
    public void shouldReturnOldValueOnDelete() throws Exception {
        store.put(hi, there);
        assertThat(store.delete(hi), equalTo(there));
    }

    @Test
    public void shouldReturnNullOnDeleteIfNoOldValue() throws Exception {
        assertThat(store.delete(hi), is(nullValue()));
    }

    @Test
    public void shouldLogKeyNullOnDelete() throws Exception {
        store.put(hi, there);
        store.delete(hi);
        assertThat(sent.get(hi), nullValue());
    }

    @Test
    public void shouldWriteToInnerOnPutIfAbsentNoPreviousValue() throws Exception {
        store.putIfAbsent(hi, there);
        assertThat(inner.get(hiBytes), equalTo(thereBytes));
    }

    @Test
    public void shouldNotWriteToInnerOnPutIfAbsentWhenValueForKeyExists() throws Exception {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        assertThat(inner.get(hiBytes), equalTo(thereBytes));
    }

    @Test
    public void shouldWriteToChangelogOnPutIfAbsentWhenNoPreviousValue() throws Exception {
        store.putIfAbsent(hi, there);
        assertThat((byte[]) sent.get(hiBytes), equalTo(thereBytes));
    }

    @Test
    public void shouldNotWriteToChangeLogOnPutIfAbsentWhenValueForKeyExists() throws Exception {
        store.put(hi, there);
        store.putIfAbsent(hi, world);
        assertThat((byte[]) sent.get(hiBytes), equalTo(thereBytes));
    }

    @Test
    public void shouldReturnCurrentValueOnPutIfAbsent() throws Exception {
        store.put(hi, there);
        assertThat(store.putIfAbsent(hi, world), equalTo(there));
    }

    @Test
    public void shouldReturnNullOnPutIfAbsentWhenNoPreviousValue() throws Exception {
        assertThat(store.putIfAbsent(hi, there), is(nullValue()));
    }

    @Test
    public void shouldQueryRange() throws Exception {
        store.put(hello, world);
        store.put(hi, there);
        store.put("zooom", "home");
        final KeyValueIterator<String, String> range = store.range(hello, hi);
        assertThat(range.next(), equalTo(KeyValue.pair(hello, world)));
        assertThat(range.next(), equalTo(KeyValue.pair(hi, there)));
        assertFalse(range.hasNext());
    }

    @Test
    public void shouldReturnAllKeyValues() throws Exception {
        store.put(hello, world);
        store.put(hi, there);
        final String zooom = "zooom";
        final String home = "home";
        store.put(zooom, home);
        final KeyValueIterator<String, String> all = store.all();
        assertThat(all.next(), equalTo(KeyValue.pair(hello, world)));
        assertThat(all.next(), equalTo(KeyValue.pair(hi, there)));
        assertThat(all.next(), equalTo(KeyValue.pair(zooom, home)));
        assertFalse(all.hasNext());
    }

    @Test
    public void shouldReturnValueOnGetWhenExists() throws Exception {
        store.put(hello, world);
        assertThat(store.get(hello), equalTo(world));
    }

    @Test
    public void shouldReturnNullOnGetWhenDoesntExist() throws Exception {
        assertThat(store.get(hello), is(nullValue()));
    }

    @Test
    public void shouldReturnInnerStoreName() throws Exception {
        assertThat(store.name(), equalTo("kv"));
    }

    private String deserializedValueFromInner(final String key) {
        return valueSerde.deserializer().deserialize("blah", inner.get(Bytes.wrap(key.getBytes())));
    }
}
