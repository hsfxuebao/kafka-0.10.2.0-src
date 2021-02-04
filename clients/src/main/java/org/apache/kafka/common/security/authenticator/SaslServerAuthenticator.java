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

package org.apache.kafka.common.security.authenticator;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.IllegalSaslStateException;
import org.apache.kafka.common.errors.UnsupportedSaslMechanismException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.network.NetworkReceive;
import org.apache.kafka.common.network.NetworkSend;
import org.apache.kafka.common.network.Send;
import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ApiVersionsResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.SaslHandshakeRequest;
import org.apache.kafka.common.requests.SaslHandshakeResponse;
import org.apache.kafka.common.security.auth.AuthCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PrincipalBuilder;
import org.apache.kafka.common.security.kerberos.KerberosName;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.ScramMechanism;
import org.apache.kafka.common.security.scram.ScramServerCallbackHandler;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SaslServerAuthenticator implements Authenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslServerAuthenticator.class);

    public enum SaslState {
        GSSAPI_OR_HANDSHAKE_REQUEST, HANDSHAKE_REQUEST, AUTHENTICATE, COMPLETE, FAILED
    }

    private final String node;
    private final Configuration jaasConfig;
    // 表示用于身份认证的主体
    private final Subject subject;
    private final KerberosShortNamer kerberosNamer;
    private final int maxReceiveSize;
    private final String host;
    private final CredentialCache credentialCache;

    // Current SASL state
    // 标识当前SaslServerAuthenticator的状态
    private SaslState saslState = SaslState.GSSAPI_OR_HANDSHAKE_REQUEST;
    // Next SASL state to be set when outgoing writes associated with the current SASL state complete
    // 用于SASL身份认证的服务端接口，在服务端使用的是Kafka提供的实现类PlainSaslServer
    private SaslState pendingSaslState = null;
    private SaslServer saslServer;
    private String saslMechanism;
    private AuthCallbackHandler callbackHandler;

    // assigned in `configure`
    // PlaintextTransportLayer对象，该字段表示底层的网络连接，其中封装了SocketChannel和SelectionKey
    private TransportLayer transportLayer;
    // 记录了服务端支持的SASL机制
    private Set<String> enabledMechanisms;
    private Map<String, ?> configs;

    // buffers used in `authenticate`
    // 读取身份认证信息的输入缓冲区
    private NetworkReceive netInBuffer;
    // 发送身份认证信息的输出缓冲区
    private Send netOutBuffer;

    public SaslServerAuthenticator(String node, Configuration jaasConfig, final Subject subject, KerberosShortNamer kerberosNameParser, String host, int maxReceiveSize, CredentialCache credentialCache) throws IOException {
        if (subject == null)
            throw new IllegalArgumentException("subject cannot be null");
        this.node = node;
        this.jaasConfig = jaasConfig;
        this.subject = subject;
        this.kerberosNamer = kerberosNameParser;
        this.maxReceiveSize = maxReceiveSize;
        this.host = host;
        this.credentialCache = credentialCache;
    }

    public void configure(TransportLayer transportLayer, PrincipalBuilder principalBuilder, Map<String, ?> configs) {
        // 初始化transportLayer
        this.transportLayer = transportLayer;
        this.configs = configs;
        // 读取配置信息，初始化enabledMechanisms字段
        List<String> enabledMechanisms = (List<String>) this.configs.get(SaslConfigs.SASL_ENABLED_MECHANISMS);
        if (enabledMechanisms == null || enabledMechanisms.isEmpty())
            throw new IllegalArgumentException("No SASL mechanisms are enabled");
        this.enabledMechanisms = new HashSet<>(enabledMechanisms);
    }

    // 创建SaslServer对象
    private void createSaslServer(String mechanism) throws IOException {
        this.saslMechanism = mechanism;
        if (!ScramMechanism.isScram(mechanism))
            callbackHandler = new SaslServerCallbackHandler(jaasConfig, kerberosNamer);
        else
            callbackHandler = new ScramServerCallbackHandler(credentialCache.cache(mechanism, ScramCredential.class));
        callbackHandler.configure(configs, Mode.SERVER, subject, saslMechanism);
        if (mechanism.equals(SaslConfigs.GSSAPI_MECHANISM)) {
            if (subject.getPrincipals().isEmpty())
                throw new IllegalArgumentException("subject must have at least one principal");
            saslServer = createSaslKerberosServer(callbackHandler, configs);
        } else {
            try {
                saslServer = Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                    public SaslServer run() throws SaslException {
                        // 调用Sasl的createSaslServer()方法
                        return Sasl.createSaslServer(saslMechanism, "kafka", host, configs, callbackHandler);
                    }
                });
            } catch (PrivilegedActionException e) {
                throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication", e.getCause());
            }
        }
    }

    private SaslServer createSaslKerberosServer(final AuthCallbackHandler saslServerCallbackHandler, final Map<String, ?> configs) throws IOException {
        // server is using a JAAS-authenticated subject: determine service principal name and hostname from kafka server's subject.
        final Principal servicePrincipal = subject.getPrincipals().iterator().next();
        KerberosName kerberosName;
        try {
            kerberosName = KerberosName.parse(servicePrincipal.getName());
        } catch (IllegalArgumentException e) {
            throw new KafkaException("Principal has name with unexpected format " + servicePrincipal);
        }
        final String servicePrincipalName = kerberosName.serviceName();
        final String serviceHostname = kerberosName.hostName();

        LOG.debug("Creating SaslServer for {} with mechanism {}", kerberosName, saslMechanism);

        // As described in http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/jgss-features.html:
        // "To enable Java GSS to delegate to the native GSS library and its list of native mechanisms,
        // set the system property "sun.security.jgss.native" to true"
        // "In addition, when performing operations as a particular Subject, for example, Subject.doAs(...)
        // or Subject.doAsPrivileged(...), the to-be-used GSSCredential should be added to Subject's
        // private credential set. Otherwise, the GSS operations will fail since no credential is found."
        boolean usingNativeJgss = Boolean.getBoolean("sun.security.jgss.native");
        if (usingNativeJgss) {
            try {
                GSSManager manager = GSSManager.getInstance();
                // This Oid is used to represent the Kerberos version 5 GSS-API mechanism. It is defined in
                // RFC 1964.
                Oid krb5Mechanism = new Oid("1.2.840.113554.1.2.2");
                GSSName gssName = manager.createName(servicePrincipalName + "@" + serviceHostname, GSSName.NT_HOSTBASED_SERVICE);
                GSSCredential cred = manager.createCredential(gssName, GSSContext.INDEFINITE_LIFETIME, krb5Mechanism, GSSCredential.ACCEPT_ONLY);
                subject.getPrivateCredentials().add(cred);
            } catch (GSSException ex) {
                LOG.warn("Cannot add private credential to subject; clients authentication may fail", ex);
            }
        }

        try {
            return Subject.doAs(subject, new PrivilegedExceptionAction<SaslServer>() {
                public SaslServer run() throws SaslException {
                    return Sasl.createSaslServer(saslMechanism, servicePrincipalName, serviceHostname, configs, saslServerCallbackHandler);
                }
            });
        } catch (PrivilegedActionException e) {
            throw new SaslException("Kafka Server failed to create a SaslServer to interact with a client during session authentication", e.getCause());
        }
    }

    /**
     * Evaluates client responses via `SaslServer.evaluateResponse` and returns the issued challenge to the client until
     * authentication succeeds or fails.
     *
     * The messages are sent and received as size delimited bytes that consists of a 4 byte network-ordered size N
     * followed by N bytes representing the opaque payload.
     */
    public void authenticate() throws IOException {
        // 如果输出缓冲区中还有未发送完成的数据，则先将这些数据发送出去
        if (netOutBuffer != null && !flushNetOutBufferAndUpdateInterestOps())
            return;

        // 检测认证是否已经完成
        if (saslServer != null && saslServer.isComplete()) {
            // 如果完成就将saslState设置为COMPLETE并直接返回
            setSaslState(SaslState.COMPLETE);
            return;
        }

        // 创建输入缓冲区
        if (netInBuffer == null) netInBuffer = new NetworkReceive(maxReceiveSize, node);

        // 从SocketChannel中读取数据
        netInBuffer.readFrom(transportLayer);

        // 检测是否读取了一个完整的信息
        if (netInBuffer.complete()) {
            netInBuffer.payload().rewind();
            byte[] clientToken = new byte[netInBuffer.payload().remaining()];
            // 获取消息载荷
            netInBuffer.payload().get(clientToken, 0, clientToken.length);
            // 重置输入缓冲区
            netInBuffer = null; // reset the networkReceive as we read all the data.
            try {
                switch (saslState) {
                    // HANDSHAKE_REQUEST和GSSAPI_OR_HANDSHAKE_REQUEST状态都会处理握手消息
                    case HANDSHAKE_REQUEST:
                        handleKafkaRequest(clientToken);
                        break;
                    case GSSAPI_OR_HANDSHAKE_REQUEST:
                        if (handleKafkaRequest(clientToken))
                            break;
                        // For default GSSAPI, fall through to authenticate using the client token as the first GSSAPI packet.
                        // This is required for interoperability with 0.9.0.x clients which do not send handshake request
                    case AUTHENTICATE:
                        // 调用PlainSaslServer的evaluateResponse()处理客户端发送过来的Response信息
                        byte[] response = saslServer.evaluateResponse(clientToken);
                        if (response != null) {
                            // 返回Challenge信息

                            netOutBuffer = new NetworkSend(node, ByteBuffer.wrap(response));
                            flushNetOutBufferAndUpdateInterestOps();
                        }
                        // When the authentication exchange is complete and no more tokens are expected from the client,
                        // update SASL state. Current SASL state will be updated when outgoing writes to the client complete.
                        if (saslServer.isComplete())
                            // 认证成功，切换状态为COMPLETE
                            setSaslState(SaslState.COMPLETE);
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                // 出现异常，切换状态为FAILED
                setSaslState(SaslState.FAILED);
                throw new IOException(e);
            }
        }
    }

    public Principal principal() {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, saslServer.getAuthorizationID());
    }

    public boolean complete() {
        return saslState == SaslState.COMPLETE;
    }

    public void close() throws IOException {
        if (saslServer != null)
            saslServer.dispose();
        if (callbackHandler != null)
            callbackHandler.close();
    }

    private void setSaslState(SaslState saslState) {
        if (netOutBuffer != null && !netOutBuffer.completed())
            pendingSaslState = saslState;
        else {
            this.pendingSaslState = null;
            this.saslState = saslState;
            LOG.debug("Set SASL server state to {}", saslState);
        }
    }

    private boolean flushNetOutBufferAndUpdateInterestOps() throws IOException {
        boolean flushedCompletely = flushNetOutBuffer();
        if (flushedCompletely) {
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);
            if (pendingSaslState != null)
                setSaslState(pendingSaslState);
        } else
            transportLayer.addInterestOps(SelectionKey.OP_WRITE);
        return flushedCompletely;
    }

    private boolean flushNetOutBuffer() throws IOException {
        if (!netOutBuffer.completed())
            netOutBuffer.writeTo(transportLayer);
        return netOutBuffer.completed();
    }

    /**
     * 处理HANDSHAKE_REQUEST和GSSAPI_OR_HANDSHAKE_REQUEST状态下的Response，检测mechanism、切换状态
     * 在握手成功后根据指定的SASL机制创建SaslServer，即PlainSaslServer
     */
    private boolean handleKafkaRequest(byte[] requestBytes) throws IOException, AuthenticationException {
        boolean isKafkaRequest = false;
        String clientMechanism = null;
        try {
            ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);
            RequestHeader requestHeader = RequestHeader.parse(requestBuffer);
            ApiKeys apiKey = ApiKeys.forId(requestHeader.apiKey());
            // A valid Kafka request header was received. SASL authentication tokens are now expected only
            // following a SaslHandshakeRequest since this is not a GSSAPI client token from a Kafka 0.9.0.x client.
            // 切换状态
            setSaslState(SaslState.HANDSHAKE_REQUEST);
            isKafkaRequest = true;

            if (!Protocol.apiVersionSupported(requestHeader.apiKey(), requestHeader.apiVersion())) {
                if (apiKey == ApiKeys.API_VERSIONS)
                    sendKafkaResponse(requestHeader, ApiVersionsResponse.fromError(Errors.UNSUPPORTED_VERSION));
                else
                    throw new UnsupportedVersionException("Version " + requestHeader.apiVersion() + " is not supported for apiKey " + apiKey);
            } else {// apiKey和version合法

                // 解析请求得到对应的请求对象
                AbstractRequest request = AbstractRequest.getRequest(requestHeader.apiKey(), requestHeader.apiVersion(), requestBuffer);

                LOG.debug("Handle Kafka request {}", apiKey);
                // 根据apiKey分别处理
                switch (apiKey) {
                    case API_VERSIONS: // 返回Broker支持的所有的ApiKey
                        handleApiVersionsRequest(requestHeader);
                        break;
                    case SASL_HANDSHAKE:  // 检测服务端是否支持客户端指定的mechanism，并返回响应
                        clientMechanism = handleHandshakeRequest(requestHeader, (SaslHandshakeRequest) request);
                        break;
                    default:
                        throw new IllegalSaslStateException("Unexpected Kafka request of type " + apiKey + " during SASL handshake.");
                }
            }
        } catch (SchemaException | IllegalArgumentException e) {
            if (saslState == SaslState.GSSAPI_OR_HANDSHAKE_REQUEST) {
                // SchemaException is thrown if the request is not in Kafka format. IllegalArgumentException is thrown
                // if the API key is invalid. For compatibility with 0.9.0.x where the first packet is a GSSAPI token
                // starting with 0x60, revert to GSSAPI for both these exceptions.
                if (LOG.isDebugEnabled()) {
                    StringBuilder tokenBuilder = new StringBuilder();
                    for (byte b : requestBytes) {
                        tokenBuilder.append(String.format("%02x", b));
                        if (tokenBuilder.length() >= 20)
                             break;
                    }
                    LOG.debug("Received client packet of length {} starting with bytes 0x{}, process as GSSAPI packet", requestBytes.length, tokenBuilder);
                }
                if (enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM)) {
                    LOG.debug("First client packet is not a SASL mechanism request, using default mechanism GSSAPI");
                    clientMechanism = SaslConfigs.GSSAPI_MECHANISM;
                } else
                    throw new UnsupportedSaslMechanismException("Exception handling first SASL packet from client, GSSAPI is not supported by server", e);
            } else
                throw e;
        }
        if (clientMechanism != null) {
            // 如果SASL机制检测通过，则创建PlainSaslServer
            createSaslServer(clientMechanism);
            // 切换为AUTHENTICATE状态
            setSaslState(SaslState.AUTHENTICATE);
        }
        return isKafkaRequest;
    }

    private String handleHandshakeRequest(RequestHeader requestHeader, SaslHandshakeRequest handshakeRequest) throws IOException, UnsupportedSaslMechanismException {
        String clientMechanism = handshakeRequest.mechanism();
        if (enabledMechanisms.contains(clientMechanism)) {
            LOG.debug("Using SASL mechanism '{}' provided by client", clientMechanism);
            sendKafkaResponse(requestHeader, new SaslHandshakeResponse((short) 0, enabledMechanisms));
            return clientMechanism;
        } else {
            LOG.debug("SASL mechanism '{}' requested by client is not supported", clientMechanism);
            sendKafkaResponse(requestHeader, new SaslHandshakeResponse(Errors.UNSUPPORTED_SASL_MECHANISM.code(), enabledMechanisms));
            throw new UnsupportedSaslMechanismException("Unsupported SASL mechanism " + clientMechanism);
        }
    }

    private void handleApiVersionsRequest(RequestHeader requestHeader) throws IOException, UnsupportedSaslMechanismException {
        sendKafkaResponse(requestHeader, ApiVersionsResponse.API_VERSIONS_RESPONSE);
    }

    private void sendKafkaResponse(RequestHeader requestHeader, AbstractResponse response) throws IOException {
        netOutBuffer = response.toSend(node, requestHeader);
        flushNetOutBufferAndUpdateInterestOps();
    }
}
