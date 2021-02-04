/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.NetworkClient;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.AuthCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Map;

public class SaslClientAuthenticator implements Authenticator {

    public enum SaslState {
        SEND_HANDSHAKE_REQUEST, RECEIVE_HANDSHAKE_RESPONSE, INITIAL, INTERMEDIATE, COMPLETE, FAILED
    }

    private static final Logger LOG = LoggerFactory.getLogger(SaslClientAuthenticator.class);

    // 表示用于身份认证的主体
    private final Subject subject;
    private final String servicePrincipal;
    private final String host;
    private final String node;
    private final String mechanism;
    private final boolean handshakeRequestEnable;

    // assigned in `configure`
    // javax.security包中提供的用于SASL身份认证的客户端接口
    private SaslClient saslClient;
    private Map<String, ?> configs;
    private String clientPrincipalName;
    // 用于收集身份认证信息的回调函数
    private AuthCallbackHandler callbackHandler;
    // PlaintextTransportLayer对象，该字段表示底层的网络连接，其中封装了SocketChannel和SelectionKey
    private TransportLayer transportLayer;

    // buffers used in `authenticate`
    // 读取身份认证信息的输入缓冲区
    private NetworkReceive netInBuffer;
    // 发送身份认证信息的输出缓冲区
    private Send netOutBuffer;

    // Current SASL state
    // 标识当前SaslClientAuthenticator的状态
    private SaslState saslState;
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    // 在输出缓冲区中的内容全部清空前，由该字段暂存下一个saslState的值
    private SaslState pendingSaslState;
    // Correlation ID for the next request
    private int correlationId;
    // Request header for which response from the server is pending
    private RequestHeader currentRequestHeader;

    public SaslClientAuthenticator(String node, Subject subject, String servicePrincipal, String host, String mechanism, boolean handshakeRequestEnable) throws IOException {
        this.node = node;
        this.subject = subject;
        this.host = host;
        this.servicePrincipal = servicePrincipal;
        this.mechanism = mechanism;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.correlationId = -1;
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) throws KafkaException {
        try {
            // PlaintextTransportLayer对象
            this.transportLayer = transportLayer;
            // 配置信息
            this.configs = configs;

            // 初始化saslState字段为SEND_HANDSHAKE_REQUEST，其pendingSaslState字段设为null
            setSaslState(handshakeRequestEnable ? SaslState.SEND_HANDSHAKE_REQUEST : SaslState.INITIAL);

            // determine client principal from subject.
            if (!subject.getPrincipals().isEmpty()) {
                Principal clientPrincipal = subject.getPrincipals().iterator().next();
                this.clientPrincipalName = clientPrincipal.getName();
            } else {
                clientPrincipalName = null;
            }
            // 用于收集认证信息的SaslClientCallbackHandler
            callbackHandler = new SaslClientCallbackHandler();
            callbackHandler.configure(configs, Mode.CLIENT, subject, mechanism);

            // 创建SaslClient对象，使用SASL/PLAIN进行身份认证时，创建的是PlainClient对象
            saslClient = createSaslClient();
        } catch (Exception e) {
            throw new KafkaException("Failed to configure SaslClientAuthenticator", e);
        }
    }

    private SaslClient createSaslClient() {
        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<SaslClient>() {
                public SaslClient run() throws SaslException {
                    String[] mechs = {mechanism};
                    LOG.debug("Creating SaslClient: client={};service={};serviceHostname={};mechs={}",
                        clientPrincipalName, servicePrincipal, host, Arrays.toString(mechs));
                    return Sasl.createSaslClient(mechs, clientPrincipalName, servicePrincipal, host, configs, callbackHandler);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new KafkaException("Failed to create SaslClient with mechanism " + mechanism, e.getCause());
        }
    }

    /**
     * Sends an empty message to the server to initiate the authentication process. It then evaluates server challenges
     * via `SaslClient.evaluateChallenge` and returns client responses until authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
     * followed by N bytes representing the opaque payload.
     */
    public void authenticate() throws IOException {
        // 发送缓冲区中还有未发送的数据，则需要先将这些数据发送完毕
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        switch (saslState) {
            case SEND_HANDSHAKE_REQUEST:
                // When multiple versions of SASL_HANDSHAKE_REQUEST are to be supported,
                // API_VERSIONS_REQUEST must be sent prior to sending SASL_HANDSHAKE_REQUEST to
                // fetch supported versions.
                String clientId = (String) configs.get(CommonClientConfigs.CLIENT_ID_CONFIG);
                // 创建并发送SaslHandshakeRequest握手消息
                SaslHandshakeRequest handshakeRequest = new SaslHandshakeRequest(mechanism);
                currentRequestHeader = new RequestHeader(ApiKeys.SASL_HANDSHAKE.id,
                        handshakeRequest.version(), clientId, correlationId++);
                send(handshakeRequest.toSend(node, currentRequestHeader));
                // 切换为RECEIVE_HANDSHAKE_RESPONSE状态
                setSaslState(SaslState.RECEIVE_HANDSHAKE_RESPONSE);
                break;
            case RECEIVE_HANDSHAKE_RESPONSE:
                // 读取SaslHandshakeResponse响应
                byte[] responseBytes = receiveResponseOrToken();
                if (responseBytes == null)   // 未读取到一个完整的消息，跳出等待下次读取
                    break;
                else {
                    // 读取到完整数据
                    try {
                        // 解析SaslHandshakeResponse响应，如果服务端返回了非零的错误码则抛出异常，否则正常返回
                        handleKafkaResponse(currentRequestHeader, responseBytes);
                        currentRequestHeader = null;
                    } catch (Exception e) {
                        setSaslState(SaslState.FAILED);
                        throw e;
                    }
                    // 切换为INITIAL状态
                    setSaslState(SaslState.INITIAL);
                    // Fall through and start SASL authentication using the configured client mechanism
                }
                // 由于这里没有break操作，且saslState切换为了INITIAL状态，因此还会继续下面的分支
            case INITIAL:
                // 发送空的byte数组，初始化身份认证流程
                sendSaslToken(new byte[0], true);
                // 设置为INTERMEDIATE状态
                setSaslState(SaslState.INTERMEDIATE);
                break;
            case INTERMEDIATE:
                // 读取服务端返回的Challenge信息
                byte[] serverToken = receiveResponseOrToken();
                if (serverToken != null) { // 读取到完整的Challenge信息
                    // 处理Challenge信息
                    sendSaslToken(serverToken, false);
                }
                // 身份认证通过
                if (saslClient.isComplete()) {
                    // 切换为COMPLETE状态
                    setSaslState(SaslState.COMPLETE);
                    transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
                }
                break;
            case COMPLETE:
                break;
            case FAILED:
                throw new IOException("SASL handshake failed");
        }
    }

    private void setSaslState(SaslState saslState) {
        if (netOutBuffer != null && !netOutBuffer.completed())
            pendingSaslState = saslState;
        else {
            this.pendingSaslState = null;
            this.saslState = saslState;
            LOG.debug("Set SASL client state to {}", saslState);
        }
    }

    // 负责处理服务端发送过来的Challenge信息，并将得到的新Response信息发送给服务端
    private void sendSaslToken(byte[] serverToken, boolean isInitial) throws IOException {
        if (!saslClient.isComplete()) {
            // 处理Challenge信息
            byte[] saslToken = createSaslToken(serverToken, isInitial);
            if (saslToken != null)
                send(new NetworkSend(node, ByteBuffer.wrap(saslToken)));
        }
    }

    private void send(Send send) throws IOException {
        try {
            // 将待发送数据封装成NetworkSend对象
            netOutBuffer = send;
            // 尝试刷新缓冲区并发送数据
            flushNetOutBufferAndUpdateInterestOps();
        } catch (IOException e) {
            setSaslState(SaslState.FAILED);
            throw e;
        }
    }

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        // 检查缓冲区的数据是否都发送完，如果没有就将其发送后再次检测并将结果返回
        boolean flushedCompletely = flushNetOutBuffer();
        if (flushedCompletely) {  // 缓冲区数据已发送完
            // 移除OP_WRITE事件
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            // 将暂存的下一个saslState的值设置到saslState上
            if (pendingSaslState != null)
                setSaslState(pendingSaslState);
        } else
            // 缓冲区数据还未发送完，继续关注OP_WRITE事件
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }

    // 从SocketChannel中读取一个完整的消息
    private byte[] receiveResponseOrToken() throws IOException {
        // 创建缓冲区
        if (netInBuffer == null) netInBuffer = new NetworkReceive(node);
        // 从SocketChannel中读取数据
        netInBuffer.readFrom(transportLayer);
        byte[] serverPacket = null;
        if (netInBuffer.complete()) { // 完成后才会状态数据到serverPacket
            netInBuffer.payload().rewind();
            serverPacket = new byte[netInBuffer.payload().remaining()];
            netInBuffer.payload().get(serverPacket, 0, serverPacket.length);
            // 清空缓冲区
            netInBuffer = null; // reset the networkReceive as we read all the data.
        }
        return serverPacket;
    }

    public Principal principal() {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, clientPrincipalName);
    }

    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    public void close() throws IOException {
        if (saslClient != null)
            saslClient.dispose();
        if (callbackHandler != null)
            callbackHandler.close();
    }

    private byte[] createSaslToken(final byte[] saslToken, boolean isInitial) throws SaslException {
        if (saslToken == null)
            throw new SaslException("Error authenticating with the Kafka Broker: received a `null` saslToken.");

        try {
            // 初始Response的处理
            if (isInitial && !saslClient.hasInitialResponse())
                return saslToken;
            else
                return Subject.doAs(subject, new PrivilegedExceptionAction<byte[]>() {
                    public byte[] run() throws SaslException {
                        // 调用SaslClient的evaluateChallenge()处理Challenge信息
                        return saslClient.evaluateChallenge(saslToken);
                    }
                });
        } catch (PrivilegedActionException e) {
            String error = "An error: (" + e + ") occurred when evaluating SASL token received from the Kafka Broker.";
            // Try to provide hints to use about what went wrong so they can fix their configuration.
            // TODO: introspect about e: look for GSS information.
            final String unknownServerErrorText =
                "(Mechanism level: Server not found in Kerberos database (7) - UNKNOWN_SERVER)";
            if (e.toString().contains(unknownServerErrorText)) {
                error += " This may be caused by Java's being unable to resolve the Kafka Broker's" +
                    " hostname correctly. You may want to try to adding" +
                    " '-Dsun.net.spi.nameservice.provider.1=dns,sun' to your client's JVMFLAGS environment." +
                    " Users must configure FQDN of kafka brokers when authenticating using SASL and" +
                    " `socketChannel.socket().getInetAddress().getHostName()` must match the hostname in `principal/hostname@realm`";
            }
            error += " Kafka Client will go to AUTH_FAILED state.";
            //Unwrap the SaslException inside `PrivilegedActionException`
            throw new SaslException(error, e.getCause());
        }
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed()) {
            // 向SocketChannel中写数据
            netOutBuffer.writeTo(transportLayer);
        }
        return netOutBuffer.completed();
    }

    private void handleKafkaResponse(RequestHeader requestHeader, byte[] responseBytes) {
        AbstractResponse response;
        ApiKeys apiKey;
        try {
            response = NetworkClient.parseResponse(ByteBuffer.wrap(responseBytes), requestHeader);
            apiKey = ApiKeys.forId(requestHeader.apiKey());
        } catch (SchemaException | IllegalArgumentException e) {
            LOG.debug("Invalid SASL mechanism response, server may be expecting only GSSAPI tokens");
            throw new AuthenticationException("Invalid SASL mechanism response", e);
        }
        switch (apiKey) {
            case SASL_HANDSHAKE:
                handleSaslHandshakeResponse((SaslHandshakeResponse) response);
                break;
            default:
                throw new IllegalStateException("Unexpected API key during handshake: " + apiKey);
        }
    }

    private void handleSaslHandshakeResponse(SaslHandshakeResponse response) {
        // 获取错误码
        Errors error = Errors.forCode(response.errorCode());
        switch (error) {
            case NONE:
                break;
            // 33，不支持的SASL mechanism
            case UNSUPPORTED_SASL_MECHANISM:
                throw new UnsupportedSaslMechanismException(String.format("Client SASL mechanism '%s' not enabled in the server, enabled mechanisms are %s",
                    mechanism, response.enabledMechanisms()));
                // 34，非法的SASL状态
            case ILLEGAL_SASL_STATE:
                throw new IllegalSaslStateException(String.format("Unexpected handshake request with client mechanism %s, enabled mechanisms are %s",
                    mechanism, response.enabledMechanisms()));
            default:
                throw new AuthenticationException(String.format("Unknown error code %d, client mechanism is %s, enabled mechanisms are %s",
                    response.errorCode(), mechanism, response.enabledMechanisms()));
        }
    }
}
