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
package org.apache.kafka.clients;

import java.util.HashMap;
import java.util.Map;

/**
 * The state of our connection to each node in the cluster.
 * 
 */
final class ClusterConnectionStates {
    private final long reconnectBackoffMs;
    private final Map<String, NodeConnectionState> nodeState;

    public ClusterConnectionStates(long reconnectBackoffMs) {
        this.reconnectBackoffMs = reconnectBackoffMs;
        this.nodeState = new HashMap<String, NodeConnectionState>();
    }

    /**
     * Return true iff we can currently initiate a new connection. This will be the case if we are not
     * connected and haven't been connected for at least the minimum reconnection backoff period.
     * @param id the connection id to check
     * @param now the current time in MS
     * @return true if we can initiate a new connection
     */
    public boolean canConnect(String id, long now) {
        //首先从缓存里面获取当前主机的连接。
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return true;
        else
            //可以从缓存里面获取到连接。
            //但是连接的状态是DISCONNECTED 并且
            // now - state.lastConnectAttemptMs >= this.reconnectBackoffMs 说明可以进行重试，重试连接。
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs >= this.reconnectBackoffMs;
    }

    /**
     * Return true if we are disconnected from the given node and can't re-establish a connection yet.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public boolean isBlackedOut(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null)
            return false;
        else
            return state.state == ConnectionState.DISCONNECTED && now - state.lastConnectAttemptMs < this.reconnectBackoffMs;
    }

    /**
     * Returns the number of milliseconds to wait, based on the connection state, before attempting to send data. When
     * disconnected, this respects the reconnect backoff time. When connecting or connected, this handles slow/stalled
     * connections.
     * @param id the connection to check
     * @param now the current time in ms
     */
    public long connectionDelay(String id, long now) {
        NodeConnectionState state = nodeState.get(id);
        if (state == null) return 0;
        long timeWaited = now - state.lastConnectAttemptMs;
        if (state.state == ConnectionState.DISCONNECTED) {
            return Math.max(this.reconnectBackoffMs - timeWaited, 0);
        } else {
            // When connecting or connected, we should be able to delay indefinitely since other events (connection or
            // data acked) will cause a wakeup once data can be sent.
            return Long.MAX_VALUE;
        }
    }

    /**
     * Return true if a specific connection establishment is currently underway
     * @param id The id of the node to check
     */
    public boolean isConnecting(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.CONNECTING;
    }

    /**
     * Enter the connecting state for the given connection.
     * @param id the id of the connection
     * @param now the current time
     */
    public void connecting(String id, long now) {
        nodeState.put(id, new NodeConnectionState(ConnectionState.CONNECTING, now));
    }

    /**
     * Enter the disconnected state for the given node.
     * @param id the connection we have disconnected
     * @param now the current time
     */
    public void disconnected(String id, long now) {
        NodeConnectionState nodeState = nodeState(id);
        /**
         * 修改主机对应的连接状态：DISCONNECTED
         * sender -> 检查网络是否可以满足发送消息的条件 -> 是否可以尝试建立网络连接
         * 如果主机的状态是DISCONNECTED 可以尝试初始化连接
         * 最后调用networkClient 的poll方法 去完成网络的连接
         */
        nodeState.state = ConnectionState.DISCONNECTED;
        nodeState.lastConnectAttemptMs = now;
    }

    /**
     * Enter the checking_api_versions state for the given node.
     * @param id the connection identifier
     */
    public void checkingApiVersions(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.CHECKING_API_VERSIONS;
    }

    /**
     * Enter the ready state for the given node.
     * @param id the connection identifier
     */
    public void ready(String id) {
        NodeConnectionState nodeState = nodeState(id);
        nodeState.state = ConnectionState.READY;
    }

    /**
     * Return true if the connection is ready.
     * @param id the connection identifier
     */
    public boolean isReady(String id) {
        NodeConnectionState state = nodeState.get(id);
        return state != null && state.state == ConnectionState.READY;
    }

    /**
     * Remove the given node from the tracked connection states. The main difference between this and `disconnected`
     * is the impact on `connectionDelay`: it will be 0 after this call whereas `reconnectBackoffMs` will be taken
     * into account after `disconnected` is called.
     *
     * @param id the connection to remove
     */
    public void remove(String id) {
        nodeState.remove(id);
    }
    
    /**
     * Get the state of a given connection.
     * @param id the id of the connection
     * @return the state of our connection
     */
    public ConnectionState connectionState(String id) {
        return nodeState(id).state;
    }
    
    /**
     * Get the state of a given node.
     * @param id the connection to fetch the state for
     */
    private NodeConnectionState nodeState(String id) {
        NodeConnectionState state = this.nodeState.get(id);
        if (state == null)
            throw new IllegalStateException("No entry found for connection " + id);
        return state;
    }
    
    /**
     * The state of our connection to a node.
     */
    private static class NodeConnectionState {

        ConnectionState state;
        long lastConnectAttemptMs;

        public NodeConnectionState(ConnectionState state, long lastConnectAttempt) {
            this.state = state;
            this.lastConnectAttemptMs = lastConnectAttempt;
        }

        public String toString() {
            return "NodeState(" + state + ", " + lastConnectAttemptMs + ")";
        }
    }
}
