/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import org.apache.kafka.streams.processor.internals.RecordContext;

/**
 * A cache entry
 */
class LRUCacheEntry implements RecordContext {

    public final byte[] value;
    private final long offset;
    private final long timestamp;
    private final String topic;
    boolean isDirty;
    private final int partition;
    private long sizeBytes = 0;


    LRUCacheEntry(final byte[] value) {
        this(value, false, -1, -1, -1, "");
    }

    LRUCacheEntry(final byte[] value, final boolean isDirty,
                  final long offset, final long timestamp, final int partition,
                  final String topic) {
        this.value = value;
        this.partition = partition;
        this.topic = topic;
        this.offset = offset;
        this.isDirty = isDirty;
        this.timestamp = timestamp;
        this.sizeBytes = (value == null ? 0 : value.length) +
                1 + // isDirty
                8 + // timestamp
                8 + // offset
                4 + // partition
                (topic == null ? 0 : topic.length());

    }



    void markClean() {
        isDirty = false;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    boolean isDirty() {
        return isDirty;
    }

    public long size() {
        return sizeBytes;
    }


}
