package org.wso2.integration.inbound.utils;

import org.apache.pulsar.client.api.Message;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.wso2.integration.inbound.pojo.ConnectionConfiguration;
import org.wso2.integration.inbound.pojo.JWTAuthConfig;
import org.wso2.integration.inbound.pojo.PulsarConnectionConfig;
import org.wso2.integration.inbound.pojo.PulsarSecureConnectionConfig;

import java.util.Properties;
import java.util.UUID;

public class PulsarUtils {

    public static PulsarConnectionConfig getPulsarConnectionConfigFromContext(Properties properties,
                                                                        PulsarConnectionConfig config)
            throws SynapseException {

        if (config == null) {
            config = new PulsarConnectionConfig();
        }

        config.setServiceUrl(properties.getProperty(PulsarConstants.SERVICE_URL));
        config.setOperationTimeoutSeconds(properties.getProperty(PulsarConstants.OPERATION_TIMEOUT_SECONDS));
        config.setStatsIntervalSeconds(properties.getProperty(PulsarConstants.STATS_INTERVAL_SECONDS));
        config.setNumIoThreads(properties.getProperty(PulsarConstants.NUM_IO_THREADS));
        config.setNumListenerThreads(properties.getProperty(PulsarConstants.NUM_LISTENER_THREADS));
        config.setUseTcpNoDelay(properties.getProperty(PulsarConstants.USE_TCP_NO_DELAY));
        config.setRequestTimeoutMs(properties.getProperty(PulsarConstants.REQUEST_TIMEOUT_MS));
        config.setMaxLookupRequest(properties.getProperty(PulsarConstants.MAX_LOOKUP_REQUESTS));
        config.setKeepAliveIntervalSeconds(properties.getProperty(PulsarConstants.KEEP_ALIVE_INTERVAL_SECONDS));
        config.setMaxBackoffIntervalNanos(properties.getProperty(PulsarConstants.MAX_BACKOFF_INTERVAL_NANOS));
        config.setConcurrentLookupRequest(properties.getProperty(PulsarConstants.CONCURRENT_LOOKUP_REQUEST));
        config.setConnectionMaxIdleSeconds(properties.getProperty(PulsarConstants.CONNECTION_MAX_IDLE_SECONDS));
        config.setConnectionTimeoutMs(properties.getProperty(PulsarConstants.CONNECTION_TIMEOUT_MS));
        config.setConnectionsPerBroker(properties.getProperty(PulsarConstants.CONNECTIONS_PER_BROKER));
        config.setEnableBusyWait(properties.getProperty(PulsarConstants.ENABLE_BUSY_WAIT));
        config.setEnableTransaction(properties.getProperty(PulsarConstants.ENABLE_TRANSACTION));
        config.setInitialBackoffIntervalNanos(properties.getProperty(PulsarConstants.INITIAL_BACKOFF_INTERVAL_NANOS));
        config.setListenerName(properties.getProperty(PulsarConstants.LISTENER_NAME));
        config.setLookupTimeoutMs(properties.getProperty(PulsarConstants.LOOKUP_TIMEOUT_MS));
        config.setMaxLookupRedirects(properties.getProperty(PulsarConstants.MAX_LOOKUP_REDIRECTS));
        config.setMaxLookupRequest(properties.getProperty(PulsarConstants.MAX_LOOKUP_REQUEST));
        config.setMaxNumberOfRejectedRequestPerConnection(properties.getProperty(PulsarConstants.MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION));
        config.setMemoryLimitBytes(properties.getProperty(PulsarConstants.MEMORY_LIMIT_BYTES));

        return config;
    }

    public static PulsarSecureConnectionConfig getPulsarSecureConnectionConfigFromContext(Properties properties)
            throws SynapseException {
        PulsarSecureConnectionConfig config = new PulsarSecureConnectionConfig();

        getPulsarConnectionConfigFromContext(properties, config);
        config.setUseTls(properties.getProperty(PulsarConstants.USE_TLS));
        config.setTlsAllowInsecureConnection(properties.getProperty(PulsarConstants.TLS_ALLOW_INSECURE_CONNECTION));
        config.setEnableTlsHostnameVerification(properties.getProperty(PulsarConstants.TLS_HOSTNAME_VERIFICATION_ENABLE));
        config.setTlsTrustCertsFilePath(properties.getProperty(PulsarConstants.TLS_TRUST_CERTS_FILE_PATH));
        config.setTlsProtocols(properties.getProperty(PulsarConstants.TLS_PROTOCOLS));
        config.setTlsCiphers(properties.getProperty(PulsarConstants.TLS_CIPHERS));
        config.setUseKeyStoreTls(properties.getProperty(PulsarConstants.USE_KEY_STORE_TLS));
        config.setTlsTrustStorePath(properties.getProperty(PulsarConstants.TLS_TRUST_STORE_PATH));
        config.setTlsTrustStorePassword(properties.getProperty(PulsarConstants.TLS_TRUST_STORE_PASSWORD));
        config.setTlsTrustStoreType(properties.getProperty(PulsarConstants.TLS_TRUST_STORE_TYPE));
        config.setAutoCertRefreshSeconds(properties.getProperty(PulsarConstants.AUTO_CERT_REFRESH_SECONDS));

        return config;
    }

    public static ConnectionConfiguration getConnectionConfigFromProperties(Properties properties) {
        ConnectionConfiguration configuration = new ConnectionConfiguration();
        configuration.setConnectionName(properties.getProperty(PulsarConstants.CONNECTION_NAME));
        configuration.setUseTlsEncryption(properties.getProperty(PulsarConstants.USE_TLS),
                properties.getProperty(PulsarConstants.SERVICE_URL));
        if (configuration.getUseTlsEncryption()) {
            PulsarSecureConnectionConfig secureConfig = getPulsarSecureConnectionConfigFromContext(properties);
            configuration.setConnectionConfig(secureConfig);
        } else {
            PulsarConnectionConfig connectionConfig = getPulsarConnectionConfigFromContext(properties, null);
            configuration.setConnectionConfig(connectionConfig);
        }

        String authType = properties.getProperty(PulsarConstants.AUTH_TYPE);
        if (authType != null) {

            switch (authType) {
                case PulsarConstants.AUTH_JWT:
                    JWTAuthConfig jwtAuthConfig = new JWTAuthConfig();
                    jwtAuthConfig.setToken(properties.getProperty(PulsarConstants.TOKEN));
                    configuration.setAuthConfig(jwtAuthConfig);
                    break;
                case PulsarConstants.AUTH_OAUTH2:
                    // Handle OAuth2 authentication
                    break;
                case PulsarConstants.AUTH_TLS:
                    // Handle TLS authentication
                    break;
                case PulsarConstants.AUTH_NONE:
                    // Handle no authentication
                    break;
                default:
                    throw new SynapseException("Unsupported authentication type: " + authType);
            }
        }

        return configuration;
    }

    /**
     * Set the Kafka Records to a MessageContext
     *
     * @return MessageContext A message context with the record header values
     */
    public static MessageContext populateMessageContext(Message<String> msg, SynapseEnvironment synapseEnvironment) {
        MessageContext msgCtx = createMessageContext(synapseEnvironment);
        msgCtx.setProperty("topic", msg.getTopicName());
        msgCtx.setProperty("msgId", msg.getMessageId());
        msgCtx.setProperty("value", msg.getValue());
        msgCtx.setProperty("key", msg.getKey());
        msgCtx.setProperty("redeliveryCount", msg.getRedeliveryCount());
        msgCtx.setProperty("properties", msg.getProperties());

        return msgCtx;
    }

    /**
     * Create the message context.
     */
    public static MessageContext createMessageContext(SynapseEnvironment synapseEnvironment) {

        MessageContext msgCtx = synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(String.valueOf(UUID.randomUUID()));
        return msgCtx;
    }

}
