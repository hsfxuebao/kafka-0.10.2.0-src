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
package org.apache.kafka.common.network;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.Configuration;

import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.security.kerberos.KerberosShortNamer;
import org.apache.kafka.common.security.authenticator.CredentialCache;
import org.apache.kafka.common.security.authenticator.LoginManager;
import org.apache.kafka.common.security.authenticator.SaslClientAuthenticator;
import org.apache.kafka.common.security.authenticator.SaslServerAuthenticator;
import org.apache.kafka.common.security.ssl.SslFactory;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.KafkaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SaslChannelBuilder implements ChannelBuilder {
    private static final Logger log = LoggerFactory.getLogger(SaslChannelBuilder.class);

    // 使用的安全协议，也即是props.put("security.protocol", "SASL_PLAINTEXT");配置的
    private final SecurityProtocol securityProtocol;
    // 使用过的SASL机制，也即是props.put("sasl.mechanism", "PLAIN");配置的
    private final String clientSaslMechanism;
    // 标识当前是客户端还是服务端，枚举值：CLIENT和SERVER
    private final Mode mode;
    // 枚举值：CLIENT和SERVER，分别是"KafkaClient"和"KafkaServer"
    private final LoginType loginType;
    // 是否发送握手消息
    private final boolean handshakeRequestEnable;
    private final CredentialCache credentialCache;

    private Configuration jaasConfig;
    // 用于封装LoginContext的LogManager对象
    private LoginManager loginManager;
    private SslFactory sslFactory;
    // 配置信息
    private Map<String, ?> configs;
    private KerberosShortNamer kerberosShortNamer;

    public SaslChannelBuilder(Mode mode, LoginType loginType, SecurityProtocol securityProtocol,
            String clientSaslMechanism, boolean handshakeRequestEnable, CredentialCache credentialCache) {
        this.mode = mode;
        this.loginType = loginType;
        this.securityProtocol = securityProtocol;
        this.handshakeRequestEnable = handshakeRequestEnable;
        this.clientSaslMechanism = clientSaslMechanism;
        this.credentialCache = credentialCache;
    }

    public void configure(Map<String, ?> configs) throws KafkaException {
        try {
            this.configs = configs;
            boolean hasKerberos;
            if (mode == Mode.SERVER) {
                List<String> enabledMechanisms = (List<String>) this.configs.get(SaslConfigs.SASL_ENABLED_MECHANISMS);
                hasKerberos = enabledMechanisms == null || enabledMechanisms.contains(SaslConfigs.GSSAPI_MECHANISM);
            } else {
                hasKerberos = clientSaslMechanism.equals(SaslConfigs.GSSAPI_MECHANISM);
            }

            if (hasKerberos) {
                String defaultRealm;
                try {
                    defaultRealm = JaasUtils.defaultKerberosRealm();
                } catch (Exception ke) {
                    defaultRealm = "";
                }
                @SuppressWarnings("unchecked")
                List<String> principalToLocalRules = (List<String>) configs.get(SaslConfigs.SASL_KERBEROS_PRINCIPAL_TO_LOCAL_RULES);
                if (principalToLocalRules != null)
                    kerberosShortNamer = KerberosShortNamer.fromUnparsedRules(defaultRealm, principalToLocalRules);
            }
            this.jaasConfig = JaasUtils.jaasConfig(loginType, configs);
            // 创建LoginManager对象，并调用其Login对象的login()方法
            this.loginManager = LoginManager.acquireLoginManager(loginType, hasKerberos, configs, jaasConfig);

            if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
                // Disable SSL client authentication as we are using SASL authentication
                this.sslFactory = new SslFactory(mode, "none");
                this.sslFactory.configure(configs);
            }
        } catch (Exception e) {
            throw new KafkaException(e);
        }
    }

    public KafkaChannel buildChannel(String id, SelectionKey key, int maxReceiveSize) throws KafkaException {
        try {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            // 创建PlaintextTransportLayer对象，它表示底层连接，其中封装了SocketChannel和SelectionKey
            TransportLayer transportLayer = buildTransportLayer(id, key, socketChannel);
            Authenticator authenticator;
            // 创建Authenticator对象，这是完成认证操作的关键
            if (mode == Mode.SERVER)
                // 服务端创建的是SaslServerAuthenticator对象
                authenticator = new SaslServerAuthenticator(id, jaasConfig, loginManager.subject(), kerberosShortNamer,
                        socketChannel.socket().getLocalAddress().getHostName(), maxReceiveSize, credentialCache);
            else
                // 客户端创建的是SaslClientAuthenticator对象
                authenticator = new SaslClientAuthenticator(id, loginManager.subject(), loginManager.serviceName(),
                        socketChannel.socket().getInetAddress().getHostName(), clientSaslMechanism, handshakeRequestEnable);
            // Both authenticators don't use `PrincipalBuilder`, so we pass `null` for now. Reconsider if this changes.
            // 通过configure()方法将TransportLayer作为参数传递过去，在SaslServerAuthenticator中会与服务端进行通信，完成身份认证
            authenticator.configure(transportLayer, null, this.configs);
            return new KafkaChannel(id, transportLayer, authenticator, maxReceiveSize);
        } catch (Exception e) {
            log.info("Failed to create channel due to ", e);
            throw new KafkaException(e);
        }
    }

    public void close()  {
        if (this.loginManager != null)
            this.loginManager.release();
    }

    protected TransportLayer buildTransportLayer(String id, SelectionKey key, SocketChannel socketChannel) throws IOException {
        if (this.securityProtocol == SecurityProtocol.SASL_SSL) {
            return SslTransportLayer.create(id, key,
                sslFactory.createSslEngine(socketChannel.socket().getInetAddress().getHostName(), socketChannel.socket().getPort()));
        } else {
            return new PlaintextTransportLayer(key);
        }
    }
}
