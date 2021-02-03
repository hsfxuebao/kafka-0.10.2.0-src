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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.errors.InterruptException;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(ConsumerNetworkClient.class);
    private static final long MAX_POLL_TIMEOUT_MS = 5000L;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    // 这里使用的是NetworkClient对象
    private final KafkaClient client;
    // 缓存队列，key是Node节点，value是发往该Node节点的ClientRequest集合
    private final Map<Node, List<ClientRequest>> unsent = new HashMap<>();
    // 集群元数据
    private final Metadata metadata;
    private final Time time;
    private final long retryBackoffMs;
    // ClientRequest在unsent中缓存的超时时长（request.timeout.ms）
    private final long unsentExpiryMs;
    /**
     * KafkaConsumer是否正在执行不可中断的方法，该值只会被KafkaConsumer线程修改
     * 每进入一个不可中断的方法，该值加1，退出不可中断的方法时，该值减1
     */
    private int wakeupDisabledCount = 0;

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    // 由调用KafkaConsumer对象的消费者线程之外的其他线程设置，表示要中断KafkaConsumer线程
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 long requestTimeoutMs) {
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.unsentExpiryMs = requestTimeoutMs;
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(long)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        long now = time.milliseconds();
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
                completionHandler);
        // 添加到unsent中，等待发送
        put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        client.wakeup();
        return completionHandler.future;
    }

    private void put(Node node, ClientRequest request) {
        synchronized (this) {
            // 先从unsent中获取有没有对应node的List<ClientRequest>集合
            List<ClientRequest> nodeUnsent = unsent.get(node);
            if (nodeUnsent == null) {
                // 如果没有则创建
                nodeUnsent = new ArrayList<>();
                unsent.put(node, nodeUnsent);
            }
            // 添加ClientRequest到对应的List<ClientRequest>集合
            nodeUnsent.add(request);
        }
    }

    public Node leastLoadedNode() {
        synchronized (this) {
            return client.leastLoadedNode(time.milliseconds());
        }
    }

    /**
     * Block until the metadata has been refreshed.
     */
    public void awaitMetadataUpdate() {
        awaitMetadataUpdate(Long.MAX_VALUE);
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(long timeout) {
        long startMs = time.milliseconds();
        // 获取当前的集群元数据版本暂存
        int version = this.metadata.requestUpdate();
        do {
            // 循环调用poll操作直到集群元数据版本发生变化
            poll(timeout);
        } while (this.metadata.version() == version && time.milliseconds() - startMs < timeout);
        return this.metadata.version() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    public void ensureFreshMetadata() {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(time.milliseconds()) == 0)
            awaitMetadataUpdate();
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is threadsafe
        log.trace("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        // 循环检测future是否完成，如果没有完成就执行poll()操作
        while (!future.isDone())
            poll(MAX_POLL_TIMEOUT_MS, time.milliseconds(), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timeout The maximum duration (in ms) to wait for the request
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, long timeout) {
        long begin = time.milliseconds();
        long remaining = timeout;
        long now = begin;
        do {
            poll(remaining, now, future);
            now = time.milliseconds();
            long elapsed = now - begin;
            remaining = timeout - elapsed;
        } while (!future.isDone() && remaining > 0);
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timeout The maximum time to wait for an IO event.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(long timeout) {
        poll(timeout, time.milliseconds(), null);
    }

    /**
     * Poll for any network IO.
     * @param timeout timeout in milliseconds
     * @param now current time in milliseconds
     */
    public void poll(long timeout, long now, PollCondition pollCondition) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        firePendingCompletedRequests();

        synchronized (this) {
            // send all the requests we can send now
            // 检测Node节点发送条件，循环处理unsent中缓存的请求，将发送请求绑定到KafkaChannel的send上，等待发送
            trySend(now);

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            if (pollCondition == null || pollCondition.shouldBlock()) {
                // if there are no requests in flight, do not block longer than the retry backoff
                if (client.inFlightRequestCount() == 0)
                    // 计算超时时间，取超时时间和delayedTasks队列中最近要执行的定时任务的时间的较小值
                    timeout = Math.min(timeout, retryBackoffMs);
                // 使用NetworkClient处理消息发送
                client.poll(Math.min(MAX_POLL_TIMEOUT_MS, timeout), now);
                now = time.milliseconds();
            } else {
                client.poll(0, now);
            }

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            // 检测消费者和每个Node之间的连接状态
            checkDisconnects(now);

            // trigger wakeups after checking for disconnects so that the callbacks will be ready
            // to be fired on the next call to poll()
            // 检测wakeup和wakeupDisabledCount，查看是否有其他线程中断
            maybeTriggerWakeup();
            
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            // 再次调用trySend循环处理unsent中缓存的请求
            trySend(now);

            // fail requests that couldn't be sent if they have expired
            // 处理unsent中超时的请求
            failExpiredRequests(now);
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        firePendingCompletedRequests();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups,
     * nor will it execute any delayed tasks.
     */
    public void pollNoWakeup() {
        // wakeupDisabledCount++
        disableWakeups();
        try {
            poll(0, time.milliseconds(), null);
        } finally {
            // wakeupDisabledCount--
            enableWakeups();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     * @param timeoutMs The maximum time in milliseconds to block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, long timeoutMs) {
        long startMs = time.milliseconds();
        long remainingMs = timeoutMs;
        /**
         * pendingRequestCount()会获取对应Node中unsent暂存的请求数量与InFlightRequests中正在发送的请求数量之和
         * 当请求还未全部完成，就一直进行poll操作
         */
        while (pendingRequestCount(node) > 0 && remainingMs > 0) {
            poll(remainingMs);
            remainingMs = timeoutMs - (time.milliseconds() - startMs);
        }

        return pendingRequestCount(node) == 0;
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        synchronized (this) {
            List<ClientRequest> pending = unsent.get(node);
            int unsentCount = pending == null ? 0 : pending.size();
            return unsentCount + client.inFlightRequestCount(node.idString());
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        synchronized (this) {
            int total = 0;
            for (List<ClientRequest> requests: unsent.values())
                total += requests.size();
            return total + client.inFlightRequestCount();
        }
    }

    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        if (completedRequestsFired)
            client.wakeup();
    }
    // 检查消费者与每个Node之间的连接状态
    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        // 遍历unsent的键值对
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            // 获取Node
            Node node = requestEntry.getKey();
            // 检查Node连接是否失败
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                // 如果失败就将相应的键值对从unsent中移除
                iterator.remove();
                // 遍历处理所有移除的ClientRequest的回调
                for (ClientRequest request : requestEntry.getValue()) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    // 注意第三个参数为true，表示这是由于断开连接而产生的回调
                    handler.onComplete(new ClientResponse(request.makeHeader(), request.callback(), request.destination(),
                            request.createdTimeMs(), now, true, null, null));
                }
            }
        }
    }

    // 处理unsent中超时的请求
    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        Iterator<Map.Entry<Node, List<ClientRequest>>> iterator = unsent.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Node, List<ClientRequest>> requestEntry = iterator.next();
            // 获取ClientRequest集合迭代器
            Iterator<ClientRequest> requestIterator = requestEntry.getValue().iterator();
            while (requestIterator.hasNext()) {
                // 得到ClientRequest
                ClientRequest request = requestIterator.next();
                /**
                 * 判断ClientRequest是否超时，判断方式
                 * 1. now - unsentExpiryMs：即从当前时间往前推unsentExpiryMs毫秒（request.timeout.ms）
                 * 2. 如果ClientRequest的创建时间还在这个时间之前，说明超时
                 */
                if (request.createdTimeMs() < now - unsentExpiryMs) {
                    // 超时处理，使用ClientRequest的handle抛出TimeoutException异常
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    handler.onFailure(new TimeoutException("Failed to send request after " + unsentExpiryMs + " ms."));
                    // 从集合中移除
                    requestIterator.remove();
                } else
                    break;
            }
            // 如果对应的ClientRequest已经空了，就将其从unsent中移除
            if (requestEntry.getValue().isEmpty())
                iterator.remove();
        }
    }

    public void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        synchronized (this) {
            List<ClientRequest> unsentRequests = unsent.remove(node);
            if (unsentRequests != null) {
                for (ClientRequest unsentRequest : unsentRequests) {
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                    handler.onFailure(e);
                }
            }
        }

        // called without the lock to avoid deadlock potential
        firePendingCompletedRequests();
    }

    // 发送unsent缓存中的ClientRequest
    private boolean trySend(long now) {
        // send any requests that can be sent now
        boolean requestsSent = false;
        // 遍历每个<Node, List<ClientRequest>>键值对
        for (Map.Entry<Node, List<ClientRequest>> requestEntry: unsent.entrySet()) {
            Node node = requestEntry.getKey();
            Iterator<ClientRequest> iterator = requestEntry.getValue().iterator();
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 检查节点是否可用
                if (client.ready(node, now)) {
                    // 将请求绑定到KafkaChannel上
                    client.send(request, now);
                    // 从集合中移除对应的ClientRequest
                    iterator.remove();
                    requestsSent = true;
                }
            }
        }
        return requestsSent;
    }

    private void maybeTriggerWakeup() {
        // 检测wakeup和wakeupDisabledCount，查看是否有其他线程中断
        if (wakeupDisabledCount == 0 && wakeup.get()) {
            log.trace("Raising wakeup exception in response to user wakeup");
            // 设置中断标志
            wakeup.set(false);
            // 如果有，抛出WakeupException，中断当前poll()方法操作
            throw new WakeupException();
        }
    }
    
    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        synchronized (this) {
            wakeupDisabledCount++;
        }
    }

    public void enableWakeups() {
        synchronized (this) {
            if (wakeupDisabledCount <= 0)
                throw new IllegalStateException("Cannot enable wakeups since they were never disabled");

            wakeupDisabledCount--;

            // re-wakeup the client if the flag was set since previous wake-up call
            // could be cleared by poll(0) while wakeups were disabled
            if (wakeupDisabledCount == 0 && wakeup.get())
                this.client.wakeup();
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            client.close();
        }
    }

    /**
     * Find whether a previous connection has failed. Note that the failure state will persist until either
     * {@link #tryConnect(Node)} or {@link #send(Node, AbstractRequest.Builder)} has been called.
     * @param node Node to connect to if possible
     */
    public boolean connectionFailed(Node node) {
        synchronized (this) {
            return client.connectionFailed(node);
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        synchronized (this) {
            // 检查Node是否准备好，如果准备好了就尝试连接
            client.ready(node, time.milliseconds());
        }
    }

    public class RequestFutureCompletionHandler implements RequestCompletionHandler {
        private final RequestFuture<ClientResponse> future;
        private ClientResponse response;
        private RuntimeException e;

        public RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        public void fireCompletion() {
            if (e != null) {
                future.raise(e);
            } else if (response.wasDisconnected()) {
                RequestHeader requestHeader = response.requestHeader();
                ApiKeys api = ApiKeys.forId(requestHeader.apiKey());
                int correlation = requestHeader.correlationId();
                log.debug("Cancelled {} request {} with correlation id {} due to node {} being disconnected",
                        api, requestHeader, correlation, response.destination());
                // 调用RequestFuture的raise()方法，传递DisconnectException异常
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                future.raise(response.versionMismatch());
            } else {
                // 否则正常完成回调，该方法来自RequestFuture类
                future.complete(response);
            }
        }

        public void onFailure(RuntimeException e) {
            this.e = e;
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            this.response = response;
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

}
