package org.wso2.integration.inbound;

import org.apache.axiom.om.OMElement;
import org.apache.axis2.builder.Builder;
import org.apache.axis2.builder.BuilderUtil;
import org.apache.axis2.builder.SOAPBuilder;
import org.apache.axis2.transport.TransportUtils;
import org.apache.commons.io.input.AutoCloseInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.synapse.MessageContext;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.SynapseException;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.wso2.carbon.inbound.endpoint.protocol.generic.GenericPollingConsumer;
import org.wso2.integration.inbound.connection.PulsarConnectionSetup;
import org.wso2.integration.inbound.pojo.ConnectionConfiguration;
import org.wso2.integration.inbound.pojo.JWTAuthConfig;
import org.wso2.integration.inbound.pojo.PulsarConnectionConfig;
import org.wso2.integration.inbound.pojo.PulsarSecureConnectionConfig;
import org.wso2.integration.inbound.utils.PulsarConstants;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class PulsarMessageConsumer extends GenericPollingConsumer {

    private static final Log log = LogFactory.getLog(PulsarMessageConsumer.class);

    private PulsarClient client;
    private Consumer consumer;
    private ConnectionConfiguration configuration;

    private String topicNames;
    private String topicsPattern;
    private String subscriptionName;
    private String subscriptionMode;
    private String subscriptionType;
    private String subscriptionInitialPosition;
    private String processingMode;
    private long ackTimeoutMillis;
    private long nackRedeliveryDelayMillis;
    private int receiverQueueSize;
    private String consumerName;
    private int priorityLevel;
    private int maxTotalReceiverQueueSizeAcrossPartitions;

    private String dlqTopic;
    private int deadLetterMaxRedeliverCount;
    // Configuration for chunked messages
    private boolean autoAckOldestChunkedMessageOnQueueFull;
    private int maxPendingChunkedMessage;
    private long expiryTimeOfIncompleteChunkedMessageMillis;

    private boolean autoUpdatePartitions;
    private boolean replicateSubscriptionState;
    private boolean readCompacted;
    private String cryptoFailureAction;
    private SortedMap<String, String> properties;

    private boolean syncReceive;
    private boolean batchingEnabled;
    private int batchingMaxMessages;
    private int batchingMaxBytes;
    private int batchingTimeout;
    private boolean batchIndexAcknowledgmentEnabled;

    private String contentType;

    private static final int THREAD_POOL_SIZE = 10;
    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public PulsarMessageConsumer(Properties properties, String name, SynapseEnvironment synapseEnvironment,
                                 long scanInterval, String injectingSeq, String onErrorSeq, boolean coordination,
                                 boolean sequential) {

        super(properties, name, synapseEnvironment, scanInterval, injectingSeq, onErrorSeq, coordination, sequential);
        configuration = getConnectionConfigFromProperties(properties);
        getConsumerConfigFromProperties(properties);
    }

    @Override
    public Object poll() {

        try {
            if (consumer == null) {
                consumer = createConsumer(configuration);
            }
            consumeMessages();

        } catch (PulsarClientException e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    private void consumeMessages() throws PulsarClientException {
        if (batchingEnabled) {
            if (syncReceive) {
                // Synchronous batch receive
                Messages<String> messages = consumer.batchReceive();  // Synchronous call
                for (Message<String> msg : messages) {
                    processMessage(msg);
                }
            } else {
                CompletableFuture<Messages<String>> future = consumer.batchReceiveAsync();

                future.thenAccept(messages -> {
                    for (Message<String> msg : messages) {
                        executor.submit(() -> processMessageAsync(msg));
                    }
                }).exceptionally(ex -> {
                    ex.printStackTrace();
                    return null;
                });
            }
        } else {
            if (syncReceive) {
                // Synchronous single message receive
                Message<String> msg = consumer.receive();  // Synchronous call
                if (msg != null) {
                    processMessage(msg);
                }
            } else {
                // Asynchronous single message receive
                CompletableFuture<Message<String>> future = consumer.receiveAsync();
                future.thenAccept(msg -> {
                    if (msg != null) {
                        executor.submit(() -> processMessageAsync(msg));
                    }
                });
            }
        }
    }

    private void processMessageAsync(Message<String> msg) {
        MessageContext msgCtx = populateMessageContext(msg);

        boolean isConsumed = injectMessage(msg.getValue(), contentType, msgCtx);

        if (isConsumed) {
            // Acknowledge the message
            consumer.acknowledgeAsync(msg);
        } else {
            // Negative ack for retry
            consumer.negativeAcknowledge(msg);
        }
    }

    private void processMessage(Message<String> msg) throws PulsarClientException {
        MessageContext msgCtx = populateMessageContext(msg);

        boolean isConsumed = injectMessage(msg.getValue(), contentType, msgCtx);

        if (isConsumed) {
            // Acknowledge the message
            consumer.acknowledge(msg);
        } else {
            // Negative ack for retry
            consumer.negativeAcknowledge(msg);
        }
    }

    private PulsarClient createPulsarConnection(ConnectionConfiguration configuration) throws PulsarClientException {
        if (client == null) {
            ClientBuilder clientBuilder = PulsarClient.builder();
            PulsarConnectionSetup connectionSetup = new PulsarConnectionSetup();
            connectionSetup.constructClientBuilder(configuration, clientBuilder);
            this.client = clientBuilder.build();
        }
        return client;
    }

    private Consumer createConsumer(ConnectionConfiguration configuration) throws PulsarClientException {
        // Logic to create and return a Pulsar consumer based on the configuration
        // This will involve using the Pulsar client library to create a consumer
        // with the specified topic, subscription, and other configurations.
        if (client == null) {
            this.client = createPulsarConnection(configuration);
        }

        ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer();

        if (topicNames != null) {
            List<String> list = Arrays.asList(topicNames.split(","));
            consumerBuilder.topics(list);
        } else if (topicsPattern != null) {
            Pattern topicPattern = Pattern.compile(topicsPattern);
            consumerBuilder.topicsPattern(topicPattern);

            if (subscriptionMode != null && !subscriptionMode.isEmpty()) {
                try {
                    consumerBuilder.subscriptionTopicsMode(RegexSubscriptionMode.valueOf(subscriptionMode));
                } catch (IllegalArgumentException e) {
                    throw new SynapseException("Invalid subscription topics mode: " + subscriptionMode, e);
                }
            }
        } else {
            throw new SynapseException("Either topicNames or topicsPattern must be specified.");
        }

        if (subscriptionName != null) {
            consumerBuilder.subscriptionName(subscriptionName);
        }
        if (subscriptionType != null) {
            try {
                consumerBuilder.subscriptionType(SubscriptionType.valueOf(subscriptionType));
            } catch (IllegalArgumentException e) {
                throw new SynapseException("Invalid subscription type: " + subscriptionType, e);
            }
        }
        if (subscriptionInitialPosition != null) {
            try {
                consumerBuilder.subscriptionInitialPosition(SubscriptionInitialPosition.valueOf(subscriptionInitialPosition));
            } catch (IllegalArgumentException e) {
                throw new SynapseException("Invalid subscription initial position: " + subscriptionInitialPosition, e);
            }
        }
        if (ackTimeoutMillis > 0) {
            consumerBuilder.ackTimeout(ackTimeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        if (consumerName != null) {
            consumerBuilder.consumerName(consumerName);
        }
        if (priorityLevel > 0) {
            consumerBuilder.priorityLevel(priorityLevel);
        }
        if (receiverQueueSize > 0) {
            consumerBuilder.receiverQueueSize(receiverQueueSize);
        }
        if (maxTotalReceiverQueueSizeAcrossPartitions > 0) {
            consumerBuilder.maxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
        }

        if (dlqTopic != null) {
            DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.builder()
                    .maxRedeliverCount(deadLetterMaxRedeliverCount)                      // Max retries before dead-lettering
                    .deadLetterTopic(dlqTopic)   // Optional: custom DLT name
                    .build();
            consumerBuilder.deadLetterPolicy(deadLetterPolicy);
        }

        if (batchingEnabled) {
            if (batchingMaxMessages <= 0 && batchingMaxBytes <= 0 && batchingTimeout <= 0) {
                throw new SynapseException("At least one of maxNumMessages, maxNumBytes, timeout must be specified.");
            }
            BatchReceivePolicy.Builder batchReceivePolicyBuilder = BatchReceivePolicy.builder();

            if (batchingMaxMessages > 0) {
                batchReceivePolicyBuilder.maxNumMessages(batchingMaxMessages);
            }
            if (batchingMaxBytes > 0) {
                batchReceivePolicyBuilder.maxNumBytes(batchingMaxBytes);
            }
            if (batchingTimeout > 0) {
                batchReceivePolicyBuilder.timeout(batchingTimeout, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            consumerBuilder.enableBatchIndexAcknowledgment(batchIndexAcknowledgmentEnabled);

            consumerBuilder.batchReceivePolicy(batchReceivePolicyBuilder.build());
        }

        if (autoAckOldestChunkedMessageOnQueueFull) {
            consumerBuilder.autoAckOldestChunkedMessageOnQueueFull(autoAckOldestChunkedMessageOnQueueFull);
        }
        if (expiryTimeOfIncompleteChunkedMessageMillis > 0) {
            consumerBuilder.expireTimeOfIncompleteChunkedMessage(expiryTimeOfIncompleteChunkedMessageMillis,
                    java.util.concurrent.TimeUnit.MILLISECONDS);
        }
        if (maxPendingChunkedMessage > 0) {
            consumerBuilder.maxPendingChunkedMessage(maxPendingChunkedMessage);
        }

        if (autoUpdatePartitions) {
            consumerBuilder.autoUpdatePartitions(autoUpdatePartitions);
        }

        if (replicateSubscriptionState) {
            consumerBuilder.replicateSubscriptionState(replicateSubscriptionState);
        }

        if (readCompacted) {
            consumerBuilder.readCompacted(readCompacted);
        }

        if (nackRedeliveryDelayMillis > 0) {
            consumerBuilder.negativeAckRedeliveryDelay(nackRedeliveryDelayMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        return consumerBuilder.subscribe();
    }

    private ConnectionConfiguration getConnectionConfigFromProperties(Properties properties) {
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

    private PulsarConnectionConfig getPulsarConnectionConfigFromContext(Properties properties,
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

    private PulsarSecureConnectionConfig getPulsarSecureConnectionConfigFromContext(Properties properties)
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

    private void getConsumerConfigFromProperties(Properties properties) {

        this.topicNames = properties.getProperty(PulsarConstants.TOPIC_NAMES);
        this.topicsPattern = properties.getProperty(PulsarConstants.TOPICS_PATTERN);
        this.subscriptionName = properties.getProperty(PulsarConstants.SUBSCRIPTION_NAME);
        this.subscriptionMode = properties.getProperty(PulsarConstants.SUBSCRIPTION_MODE);
        this.subscriptionType = properties.getProperty(PulsarConstants.SUBSCRIPTION_TYPE);
        this.subscriptionInitialPosition = properties.getProperty(PulsarConstants.SUBSCRIPTION_INITIAL_POSITION);
        this.ackTimeoutMillis = Long.parseLong(properties.getProperty(PulsarConstants.ACK_TIMEOUT_MILLIS));
        this.nackRedeliveryDelayMillis = Long.parseLong(properties.getProperty(PulsarConstants.NACK_REDELIVERY_DELAY));
        this.receiverQueueSize = Integer.parseInt(properties.getProperty(PulsarConstants.RECEIVER_QUEUE_SIZE));
        this.consumerName = properties.getProperty(PulsarConstants.CONSUMER_NAME);
        this.priorityLevel = Integer.parseInt(properties.getProperty(PulsarConstants.PRIORITY_LEVEL));
        this.maxTotalReceiverQueueSizeAcrossPartitions = Integer.parseInt(
                properties.getProperty(PulsarConstants.MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS));
        this.deadLetterMaxRedeliverCount = Integer.parseInt(properties.getProperty(PulsarConstants.DLQ_MAX_REDELIVERY_COUNT, "5"));
        this.dlqTopic = properties.getProperty(PulsarConstants.DLQ_TOPIC);
        this.autoAckOldestChunkedMessageOnQueueFull = Boolean.parseBoolean(
                properties.getProperty(PulsarConstants.AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL));
        this.maxPendingChunkedMessage = Integer.parseInt(
                properties.getProperty(PulsarConstants.MAX_PENDING_CHUNKED_MESSAGE));
        this.expiryTimeOfIncompleteChunkedMessageMillis = Long.parseLong(
                properties.getProperty(PulsarConstants.EXPIRY_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS));
        this.autoUpdatePartitions = Boolean.parseBoolean(
                properties.getProperty(PulsarConstants.AUTO_UPDATE_PARTITIONS));
        this.replicateSubscriptionState = Boolean.parseBoolean(
                properties.getProperty(PulsarConstants.REPLICATE_SUBSCRIPTION_STATE));
        this.readCompacted = Boolean.parseBoolean(properties.getProperty(PulsarConstants.READ_COMPACTED));
        this.cryptoFailureAction = properties.getProperty(PulsarConstants.CRYPTO_FAILURE_ACTION);
        this.batchingEnabled = Boolean.parseBoolean(properties.getProperty(PulsarConstants.BATCHING_ENABLED));
        this.batchingMaxMessages = Integer.parseInt(properties.getProperty(PulsarConstants.BATCHING_MAX_MESSAGES));
        this.batchingMaxBytes = Integer.parseInt(properties.getProperty(PulsarConstants.BATCHING_MAX_BYTES));
        this.batchingTimeout = Integer.parseInt(properties.getProperty(PulsarConstants.BATCHING_TIMEOUT));
        this.batchIndexAcknowledgmentEnabled = Boolean.parseBoolean(
                properties.getProperty(PulsarConstants.BATCH_INDEX_ACK_ENABLED));
        this.processingMode = properties.getProperty(PulsarConstants.PROCESSING_MODE);
        this.syncReceive = PulsarConstants.SYNC.equalsIgnoreCase(properties.getProperty(PulsarConstants.PROCESSING_MODE));
        this.contentType = properties.getProperty("contentType");

    }

    /**
     * Set the Kafka Records to a MessageContext
     *
     * @return MessageContext A message context with the record header values
     */
    private MessageContext populateMessageContext(Message<String> msg) {
        MessageContext msgCtx = createMessageContext();
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
    private MessageContext createMessageContext() {

        MessageContext msgCtx = this.synapseEnvironment.createMessageContext();
        org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx).getAxis2MessageContext();
        axis2MsgCtx.setServerSide(true);
        axis2MsgCtx.setMessageID(String.valueOf(UUID.randomUUID()));
        return msgCtx;
    }

    private boolean injectMessage(String strMessage, String contentType, MessageContext msgCtx) {

        AutoCloseInputStream in = new AutoCloseInputStream(new ByteArrayInputStream(strMessage.getBytes()));
        return this.injectMessage(in, contentType, msgCtx);
    }

    private boolean injectMessage(InputStream in, String contentType, MessageContext msgCtx) {
        boolean isConsumed = true;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Processed Custom inbound EP Message of Content-type : " + contentType + " for " + name);
            }

            org.apache.axis2.context.MessageContext axis2MsgCtx = ((Axis2MessageContext) msgCtx)
                    .getAxis2MessageContext();
            Object builder;
            if (StringUtils.isEmpty(contentType)) {
                log.warn("Unable to determine content type for message, setting to application/json for " + name);
            }
            int index = contentType.indexOf(';');
            String type = index > 0 ? contentType.substring(0, index) : contentType;
            builder = BuilderUtil.getBuilderFromSelector(type, axis2MsgCtx);
            if (builder == null) {
                if (log.isDebugEnabled()) {
                    log.debug("No message builder found for type '" + type + "'. Falling back to SOAP. for" + name);
                }
                builder = new SOAPBuilder();
            }

            OMElement documentElement1 = ((Builder) builder).processDocument(in, contentType, axis2MsgCtx);
            msgCtx.setEnvelope(TransportUtils.createSOAPEnvelope(documentElement1));
            if (this.injectingSeq == null || "".equals(this.injectingSeq)) {
                log.error("Sequence name not specified. Sequence : " + this.injectingSeq + " for " + name);
                isConsumed = false;
            }

            SequenceMediator seq = (SequenceMediator) this.synapseEnvironment.getSynapseConfiguration().getSequence(
                    this.injectingSeq);
            if (seq == null) {
                throw new SynapseException(
                        "Sequence with name : " + this.injectingSeq + " is not found to mediate the message.");
            }
            seq.setErrorHandler(this.onErrorSeq);
            if (log.isDebugEnabled()) {
                log.debug("injecting message to sequence : " + this.injectingSeq + " of " + name);
            }
            if (!this.synapseEnvironment.injectInbound(msgCtx, seq, this.sequential)) {
                isConsumed = false;
            }
            if (isRollback(msgCtx)) {
                isConsumed = false;
            }
        } catch (Exception e) {
            log.error("Error while processing the Kafka inbound endpoint Message and the message should be in the "
                    + "format of " + contentType, e);
            isConsumed = false;
        }
        return isConsumed;
    }

    /**
     * Check the SET_ROLLBACK_ONLY property set to true
     *
     * @param msgCtx SynapseMessageContext
     * @return true or false
     */
    private boolean isRollback(MessageContext msgCtx) {
        // check rollback property from synapse context
        Object rollbackProp = msgCtx.getProperty("SET_ROLLBACK_ONLY");
        if (rollbackProp != null) {
            return (rollbackProp instanceof Boolean && ((Boolean) rollbackProp))
                    || (rollbackProp instanceof String && Boolean.valueOf((String) rollbackProp));
        }
        return false;
    }
}
