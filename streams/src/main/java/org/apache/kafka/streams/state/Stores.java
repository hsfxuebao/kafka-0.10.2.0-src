/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.state;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.InMemoryLRUCacheStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBKeyValueStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBSessionStoreSupplier;
import org.apache.kafka.streams.state.internals.RocksDBWindowStoreSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory for creating state stores in Kafka Streams.
 */
public class Stores {

    private static final Logger log = LoggerFactory.getLogger(Stores.class);

    /**
     * Begin to create a new {@link org.apache.kafka.streams.processor.StateStoreSupplier} instance.
     *
     * @param name the name of the store
     * @return the factory that can be used to specify other options or configurations for the store; never null
     */
    public static StoreFactory create(final String name) {
        return new StoreFactory() {
            @Override
            public <K> ValueFactory<K> withKeys(final Serde<K> keySerde) {
                return new ValueFactory<K>() {
                    @Override
                    public <V> KeyValueFactory<K, V> withValues(final Serde<V> valueSerde) {

                        return new KeyValueFactory<K, V>() {

                            @Override
                            public InMemoryKeyValueFactory<K, V> inMemory() {
                                return new InMemoryKeyValueFactory<K, V>() {
                                    private int capacity = Integer.MAX_VALUE;
                                    private final Map<String, String> logConfig = new HashMap<>();
                                    private boolean logged = true;

                                    /**
                                     * @param capacity the maximum capacity of the in-memory cache; should be one less than a power of 2
                                     * @throws IllegalArgumentException if the capacity of the store is zero or negative
                                     */
                                    @Override
                                    public InMemoryKeyValueFactory<K, V> maxEntries(int capacity) {
                                        if (capacity < 1) throw new IllegalArgumentException("The capacity must be positive");
                                        this.capacity = capacity;
                                        return this;
                                    }

                                    @Override
                                    public InMemoryKeyValueFactory<K, V> enableLogging(final Map<String, String> config) {
                                        logged = true;
                                        logConfig.putAll(config);
                                        return this;
                                    }

                                    @Override
                                    public InMemoryKeyValueFactory<K, V> disableLogging() {
                                        logged = false;
                                        logConfig.clear();
                                        return this;
                                    }

                                    @Override
                                    public StateStoreSupplier build() {
                                        log.trace("Creating InMemory Store name={} capacity={} logged={}", name, capacity, logged);
                                        if (capacity < Integer.MAX_VALUE) {
                                            return new InMemoryLRUCacheStoreSupplier<>(name, capacity, keySerde, valueSerde, logged, logConfig);
                                        }
                                        return new InMemoryKeyValueStoreSupplier<>(name, keySerde, valueSerde, logged, logConfig);
                                    }
                                };
                            }

                            @Override
                            public PersistentKeyValueFactory<K, V> persistent() {
                                return new PersistentKeyValueFactory<K, V>() {
                                    public boolean cachingEnabled;
                                    private long windowSize;
                                    private final Map<String, String> logConfig = new HashMap<>();
                                    private int numSegments = 0;
                                    private long retentionPeriod = 0L;
                                    private boolean retainDuplicates = false;
                                    private boolean sessionWindows;
                                    private boolean logged = true;

                                    @Override
                                    public PersistentKeyValueFactory<K, V> windowed(final long windowSize, final long retentionPeriod, final int numSegments, final boolean retainDuplicates) {
                                        this.windowSize = windowSize;
                                        this.numSegments = numSegments;
                                        this.retentionPeriod = retentionPeriod;
                                        this.retainDuplicates = retainDuplicates;
                                        this.sessionWindows = false;

                                        return this;
                                    }

                                    @Override
                                    public PersistentKeyValueFactory<K, V> sessionWindowed(final long retentionPeriod) {
                                        this.sessionWindows = true;
                                        this.retentionPeriod = retentionPeriod;
                                        return this;
                                    }

                                    @Override
                                    public PersistentKeyValueFactory<K, V> enableLogging(final Map<String, String> config) {
                                        logged = true;
                                        logConfig.putAll(config);
                                        return this;
                                    }

                                    @Override
                                    public PersistentKeyValueFactory<K, V> disableLogging() {
                                        logged = false;
                                        logConfig.clear();
                                        return this;
                                    }

                                    @Override
                                    public PersistentKeyValueFactory<K, V> enableCaching() {
                                        cachingEnabled = true;
                                        return this;
                                    }

                                    @Override
                                    public StateStoreSupplier build() {
                                        log.trace("Creating RocksDb Store name={} numSegments={} logged={}", name, numSegments, logged);
                                        if (sessionWindows) {
                                            return new RocksDBSessionStoreSupplier<>(name, retentionPeriod, keySerde, valueSerde, logged, logConfig, cachingEnabled);
                                        } else if (numSegments > 0) {
                                            return new RocksDBWindowStoreSupplier<>(name, retentionPeriod, numSegments, retainDuplicates, keySerde, valueSerde, windowSize, logged, logConfig, cachingEnabled);
                                        }
                                        return new RocksDBKeyValueStoreSupplier<>(name, keySerde, valueSerde, logged, logConfig, cachingEnabled);
                                    }

                                };
                            }


                        };
                    }
                };
            }
        };
    }


    public static abstract class StoreFactory {
        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link String}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<String> withStringKeys() {
            return withKeys(Serdes.String());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Integer}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Integer> withIntegerKeys() {
            return withKeys(Serdes.Integer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Long}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Long> withLongKeys() {
            return withKeys(Serdes.Long());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link Double}s.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<Double> withDoubleKeys() {
            return withKeys(Serdes.Double());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be {@link ByteBuffer}.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<ByteBuffer> withByteBufferKeys() {
            return withKeys(Serdes.ByteBuffer());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys will be byte arrays.
         *
         * @return the interface used to specify the type of values; never null
         */
        public ValueFactory<byte[]> withByteArrayKeys() {
            return withKeys(Serdes.ByteArray());
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the keys.
         *
         * @param keyClass the class for the keys, which must be one of the types for which Kafka has built-in serdes
         * @return the interface used to specify the type of values; never null
         */
        public <K> ValueFactory<K> withKeys(Class<K> keyClass) {
            return withKeys(Serdes.serdeFrom(keyClass));
        }

        /**
         * Begin to create a {@link KeyValueStore} by specifying the serializer and deserializer for the keys.
         *
         * @param keySerde  the serialization factory for keys; may be null
         * @return          the interface used to specify the type of values; never null
         */
        public abstract <K> ValueFactory<K> withKeys(Serde<K> keySerde);
    }

    /**
     * The factory for creating off-heap key-value stores.
     *
     * @param <K> the type of keys
     */
    public static abstract class ValueFactory<K> {
        /**
         * Use {@link String} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, String> withStringValues() {
            return withValues(Serdes.String());
        }

        /**
         * Use {@link Integer} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Integer> withIntegerValues() {
            return withValues(Serdes.Integer());
        }

        /**
         * Use {@link Long} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Long> withLongValues() {
            return withValues(Serdes.Long());
        }

        /**
         * Use {@link Double} values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, Double> withDoubleValues() {
            return withValues(Serdes.Double());
        }

        /**
         * Use {@link ByteBuffer} for values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, ByteBuffer> withByteBufferValues() {
            return withValues(Serdes.ByteBuffer());
        }

        /**
         * Use byte arrays for values.
         *
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public KeyValueFactory<K, byte[]> withByteArrayValues() {
            return withValues(Serdes.ByteArray());
        }

        /**
         * Use values of the specified type.
         *
         * @param valueClass the class for the values, which must be one of the types for which Kafka has built-in serdes
         * @return the interface used to specify the remaining key-value store options; never null
         */
        public <V> KeyValueFactory<K, V> withValues(Class<V> valueClass) {
            return withValues(Serdes.serdeFrom(valueClass));
        }

        /**
         * Use the specified serializer and deserializer for the values.
         *
         * @param valueSerde    the serialization factory for values; may be null
         * @return              the interface used to specify the remaining key-value store options; never null
         */
        public abstract <V> KeyValueFactory<K, V> withValues(Serde<V> valueSerde);
    }


    public interface KeyValueFactory<K, V> {
        /**
         * Keep all key-value entries in-memory, although for durability all entries are recorded in a Kafka topic that can be
         * read to restore the entries if they are lost.
         *
         * @return the factory to create in-memory key-value stores; never null
         */
        InMemoryKeyValueFactory<K, V> inMemory();

        /**
         * Keep all key-value entries off-heap in a local database, although for durability all entries are recorded in a Kafka
         * topic that can be read to restore the entries if they are lost.
         *
         * @return the factory to create persistent key-value stores; never null
         */
        PersistentKeyValueFactory<K, V> persistent();
    }

    /**
     * The interface used to create in-memory key-value stores.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface InMemoryKeyValueFactory<K, V> {
        /**
         * Limits the in-memory key-value store to hold a maximum number of entries. The default is {@link Integer#MAX_VALUE}, which is
         * equivalent to not placing a limit on the number of entries.
         *
         * @param capacity the maximum capacity of the in-memory cache; should be one less than a power of 2
         * @return this factory
         * @throws IllegalArgumentException if the capacity is not positive
         */
        InMemoryKeyValueFactory<K, V> maxEntries(int capacity);

        /**
         * Indicates that a changelog should be created for the store. The changelog will be created
         * with the provided cleanupPolicy and configs.
         *
         * Note: Any unrecognized configs will be ignored.
         * @param config    any configs that should be applied to the changelog
         * @return  the factory to create an in-memory key-value store
         */
        InMemoryKeyValueFactory<K, V> enableLogging(final Map<String, String> config);

        /**
         * Indicates that a changelog should not be created for the key-value store
         * @return the factory to create an in-memory key-value store
         */
        InMemoryKeyValueFactory<K, V> disableLogging();


        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the state store supplier; never null
         */
        StateStoreSupplier build();
    }

    /**
     * The interface used to create off-heap key-value stores that use a local database.
     *
     * @param <K> the type of keys
     * @param <V> the type of values
     */
    public interface PersistentKeyValueFactory<K, V> {

        /**
         * Set the persistent store as a windowed key-value store
         * @param windowSize size of the windows
         * @param retentionPeriod the maximum period of time in milli-second to keep each window in this store
         * @param numSegments the maximum number of segments for rolling the windowed store
         * @param retainDuplicates whether or not to retain duplicate data within the window
         */
        PersistentKeyValueFactory<K, V> windowed(final long windowSize, long retentionPeriod, int numSegments, boolean retainDuplicates);

        /**
         * Set the persistent store as a {@link SessionStore} for use with {@link org.apache.kafka.streams.kstream.SessionWindows}
         * @param retentionPeriod period of time in milliseconds to keep each window in this store
         */
        PersistentKeyValueFactory<K, V> sessionWindowed(final long retentionPeriod);

        /**
         * Indicates that a changelog should be created for the store. The changelog will be created
         * with the provided cleanupPolicy and configs.
         *
         * Note: Any unrecognized configs will be ignored.
         * @param config            any configs that should be applied to the changelog
         * @return  the factory to create a persistent key-value store
         */
        PersistentKeyValueFactory<K, V> enableLogging(final Map<String, String> config);

        /**
         * Indicates that a changelog should not be created for the key-value store
         * @return the factory to create a persistent key-value store
         */
        PersistentKeyValueFactory<K, V> disableLogging();

        /**
         * Caching should be enabled on the created store.
         * @return the factory to create a persistent key-value store
         */
        PersistentKeyValueFactory<K, V> enableCaching();
        /**
         * Return the instance of StateStoreSupplier of new key-value store.
         * @return the key-value store; never null
         */
        StateStoreSupplier build();

    }
}

