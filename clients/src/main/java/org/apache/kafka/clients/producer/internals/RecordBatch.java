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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    final long createdMs;
    // 当前RecordBatch中缓存的消息都会发送给次TopicPartition
    final TopicPartition topicPartition;
    // 标识RecordBatch状态的Future对象
    final ProduceRequestResult produceFuture;

    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;

    // 尝试发送当前RecordBatch的次数
    volatile int attempts;
    // 记录保存的Record数量
    int recordCount;
    int maxRecordSize;
    long drainedMs;
    // 最后一次尝试发送的时间戳
    long lastAttemptMs;
    // 最后一次向RecordBatch追加消息的时间戳
    long lastAppendTime;
    private String expiryErrorMessage;
    private AtomicBoolean completed;
    // 是否正在重试
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.completed = new AtomicBoolean();
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        // 估算剩余空间是否足够
        if (!recordsBuilder.hasRoomFor(key, value)) {
            return null;
        } else {
            // 向MemoryRecords中添加数据，offsetCounter是在RecordBatch中的偏移量
            //TODO 往批次里面去写数据
            long checksum = this.recordsBuilder.append(timestamp, key, value);
            // 记录最大消息大小的字节数，这个值会不断更新，始终记录已添加的消息记录中最大的那条的大小
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            // 更新最近添加时间
            this.lastAppendTime = now;
            // 将消息构造一个FutureRecordMetadata对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            // 如果callback不会空，就将上面得到的FutureRecordMetadata和该callback包装为一个thunk，放到thunks集合里
            if (callback != null)
                thunks.add(new Thunk(callback, future));
            // 更新保存的记录数量
            this.recordCount++;
            // 返回FutureRecordMetadata对象
            return future;
        }
    }

    /**
     * Complete the request.
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     */
    public void done(long baseOffset, long logAppendTime, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition, baseOffset, exception);

        if (completed.getAndSet(true))
            throw new IllegalStateException("Batch has already been completed");

        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        /**
         *
         * 我们发送数据的时候，一条消息就代表一个thunk
         * 遍历所有我们当时发送出去的消息。
         */
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    RecordMetadata metadata = thunk.future.value();
                    //调用我们发送的消息的回调函数
                    //大家还记不记得我们在发送数据的时候
                    //还不是绑定了一个回调函数。
                    //这儿说的调用的回调函数
                    //就是我们开发，生产者代码的时候，我们用户传进去的那个回调函数。

                    thunk.callback.onCompletion(metadata, null);
                    //带过去的就是没有异常
                    //也就是说我们生产者那儿的代码，捕获异常的时候就是发现没有异常。
                } else {
                    //如果有异常就会把异常传给回调函数。
                    //由我们用户自己去捕获这个异常。
                    //然后对这个异常进行处理
                    //大家根据自己公司的业务规则进行处理就可以了。

                    //如果走这个分支的话，我们的用户的代码是可以捕获到timeoutexception
                    //这个异常，如果用户捕获到了，做对应的处理就可以了。
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        produceFuture.done();
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     * This methods closes this batch and sets {@code expiryErrorMessage} if the batch has timed out.
     * {@link #expirationDone()} must be invoked to complete the produce future and invoke callbacks.
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {

        /**
         * requestTimeoutMs：代表的是请求发送的超时的时间。默认值是30.
         * now：当前时间
         * lastAppendTime：批次的创建的时间（上一次重试的时间）
         * now - this.lastAppendTime 大于30秒，说明批次超时了 还没发送出去。
         */
        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            // 记录异常信息
            expiryErrorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
        /**
         * lingerMs: 默认0  我们 一般都会设置100ms，无论如何都要把消息发送出去的时间
         * createdMs:批次创建的时间
         * 已经大于30秒了。 说明也是超时了。
         */
        else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs)))
            expiryErrorMessage = (now - (this.createdMs + lingerMs)) + " ms has passed since batch creation plus linger time";
        /**
         * 针对重试
         * lastAttemptMs： 上一次重试的时间（批次创建的时间）
         * retryBackoffMs： 重试的时间间隔
         * 说明也是超时了。
         */
        else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs)))
            expiryErrorMessage = (now - (this.lastAttemptMs + retryBackoffMs)) + " ms has passed since last attempt plus backoff time";

        boolean expired = expiryErrorMessage != null;
        if (expired)
            close();
        return expired;
    }

    /**
     * Completes the produce future with timeout exception and invokes callbacks.
     * This method should be invoked only if {@link #maybeExpire(int, long, long, long, boolean)}
     * returned true.
     */
    void expirationDone() {
        if (expiryErrorMessage == null)
            throw new IllegalStateException("Batch has not expired");
        //调用done方法
        //方法里面传过去了一个TimeoutException的异常。（超时了
        this.done(-1L, Record.NO_TIMESTAMP,
                  new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + ": " + expiryErrorMessage));
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    private boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int sizeInBytes() {
        return recordsBuilder.sizeInBytes();
    }

    public double compressionRate() {
        return recordsBuilder.compressionRate();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void close() {
        recordsBuilder.close();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

}
