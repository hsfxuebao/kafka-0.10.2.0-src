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
package org.apache.kafka.test;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlySessionStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReadOnlySessionStoreStub<K, V> implements ReadOnlySessionStore<K, V>, StateStore {
    private Map<K, List<KeyValue<Windowed<K>, V>>> sessions = new HashMap<>();
    private boolean open = true;

    public void put(final Windowed<K> sessionKey, final V value) {
        if (!sessions.containsKey(sessionKey.key())) {
            sessions.put(sessionKey.key(), new ArrayList<KeyValue<Windowed<K>, V>>());
        }
        sessions.get(sessionKey.key()).add(KeyValue.pair(sessionKey, value));
    }

    @Override
    public KeyValueIterator<Windowed<K>, V> fetch(final K key) {
        if (!open) {
            throw new InvalidStateStoreException("not open");
        }
        if (!sessions.containsKey(key)) {
            return new KeyValueIteratorStub<>(Collections.<KeyValue<Windowed<K>, V>>emptyIterator());
        }
        return new KeyValueIteratorStub<>(sessions.get(key).iterator());
    }

    @Override
    public String name() {
        return "";
    }

    @Override
    public void init(final ProcessorContext context, final StateStore root) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return open;
    }


    public void setOpen(final boolean open) {
        this.open = open;
    }
}
