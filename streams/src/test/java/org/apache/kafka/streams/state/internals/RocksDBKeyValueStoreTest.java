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
package org.apache.kafka.streams.state.internals;

import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueStoreTestDriver;
import org.apache.kafka.streams.state.RocksDBConfigSetter;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockProcessorContext;
import org.junit.Test;
import org.rocksdb.Options;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @SuppressWarnings("unchecked")
    @Override
    protected <K, V> KeyValueStore<K, V> createKeyValueStore(
            ProcessorContext context,
            Class<K> keyClass,
            Class<V> valueClass,
            boolean useContextSerdes) {

        return createStore(context, keyClass, valueClass, useContextSerdes, false);

    }

    @SuppressWarnings("unchecked")
    private <K, V> KeyValueStore<K, V> createStore(final ProcessorContext context, final Class<K> keyClass, final Class<V> valueClass, final boolean useContextSerdes, final boolean enableCaching) {

        Stores.PersistentKeyValueFactory<?, ?> factory;
        if (useContextSerdes) {
            factory = Stores
                    .create("my-store")
                    .withKeys(context.keySerde())
                    .withValues(context.valueSerde())
                    .persistent();

        } else {
            factory = Stores
                    .create("my-store")
                    .withKeys(keyClass)
                    .withValues(valueClass)
                    .persistent();
        }

        if (enableCaching) {
            factory.enableCaching();
        }
        KeyValueStore<K, V> store = (KeyValueStore<K, V>) factory.build().get();
        store.init(context, store);
        return store;
    }

    public static class TheRocksDbConfigSetter implements RocksDBConfigSetter {

        static boolean called = false;

        @Override
        public void setConfig(final String storeName, final Options options, final Map<String, Object> configs) {
            called = true;
        }
    }

    @Test
    public void shouldUseCustomRocksDbConfigSetter() throws Exception {
        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        driver.setConfig(StreamsConfig.ROCKSDB_CONFIG_SETTER_CLASS_CONFIG, TheRocksDbConfigSetter.class);
        createKeyValueStore(driver.context(), Integer.class, String.class, false);
        assertTrue(TheRocksDbConfigSetter.called);
    }

    @Test
    public void shouldPerformRangeQueriesWithCachingDisabled() throws Exception {
        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        final MockProcessorContext context = (MockProcessorContext) driver.context();
        final KeyValueStore<Integer, String> store = createStore(context, Integer.class, String.class, false, false);
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> range = store.range(1, 2);
        assertEquals("hi", range.next().value);
        assertEquals("goodbye", range.next().value);
        assertFalse(range.hasNext());
    }

    @Test
    public void shouldPerformAllQueriesWithCachingDisabled() throws Exception {
        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        final MockProcessorContext context = (MockProcessorContext) driver.context();
        final KeyValueStore<Integer, String> store = createStore(context, Integer.class, String.class, false, false);
        context.setTime(1L);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> range = store.all();
        assertEquals("hi", range.next().value);
        assertEquals("goodbye", range.next().value);
        assertFalse(range.hasNext());
    }

    @Test
    public void shouldCloseOpenIteratorsWhenStoreClosedAndThrowInvalidStateStoreOnHasNextAndNext() throws Exception {
        final KeyValueStoreTestDriver<Integer, String> driver = KeyValueStoreTestDriver.create(Integer.class, String.class);
        final MockProcessorContext context = (MockProcessorContext) driver.context();
        context.setTime(1L);
        final KeyValueStore<Integer, String> store = createStore(context, Integer.class, String.class, false, false);
        store.put(1, "hi");
        store.put(2, "goodbye");
        final KeyValueIterator<Integer, String> iteratorOne = store.range(1, 5);
        final KeyValueIterator<Integer, String> iteratorTwo = store.range(1, 4);

        assertTrue(iteratorOne.hasNext());
        assertTrue(iteratorTwo.hasNext());

        store.close();

        try {
            iteratorOne.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorOne.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.hasNext();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

        try {
            iteratorTwo.next();
            fail("should have thrown InvalidStateStoreException on closed store");
        } catch (InvalidStateStoreException e) {
            // ok
        }

    }

}
