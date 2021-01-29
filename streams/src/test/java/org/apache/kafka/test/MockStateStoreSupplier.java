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

package org.apache.kafka.test;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreSupplier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class MockStateStoreSupplier implements StateStoreSupplier {
    private String name;
    private boolean persistent;
    private boolean loggingEnabled;
    private MockStateStore stateStore;

    public MockStateStoreSupplier(String name, boolean persistent) {
        this(name, persistent, true);
    }

    public MockStateStoreSupplier(String name, boolean persistent, boolean loggingEnabled) {
        this.name = name;
        this.persistent = persistent;
        this.loggingEnabled = loggingEnabled;
    }

    public MockStateStoreSupplier(final MockStateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public StateStore get() {
        if (stateStore != null) {
            return stateStore;
        }
        if (loggingEnabled) {
            return new MockStateStore(name, persistent).enableLogging();
        } else {
            return new MockStateStore(name, persistent);
        }
    }

    @Override
    public Map<String, String> logConfig() {
        return Collections.emptyMap();
    }

    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    public static class MockStateStore implements StateStore {
        private final String name;
        private final boolean persistent;

        public boolean loggingEnabled = false;
        public boolean initialized = false;
        public boolean flushed = false;
        public boolean closed = true;
        public final ArrayList<Integer> keys = new ArrayList<>();

        public MockStateStore(String name, boolean persistent) {
            this.name = name;
            this.persistent = persistent;
        }

        public MockStateStore enableLogging() {
            loggingEnabled = true;
            return this;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public void init(ProcessorContext context, StateStore root) {
            context.register(root, loggingEnabled, stateRestoreCallback);
            initialized = true;
            closed = false;
        }

        @Override
        public void flush() {
            flushed = true;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean persistent() {
            return persistent;
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        public final StateRestoreCallback stateRestoreCallback = new StateRestoreCallback() {
            private final Deserializer<Integer> deserializer = new IntegerDeserializer();

            @Override
            public void restore(byte[] key, byte[] value) {
                keys.add(deserializer.deserialize("", key));
            }
        };
    }
}
