package org.wso2.integration.inbound.utils;

public class PulsarConstants {

    // Pulsar connection parameter constants
    public static final String CONNECTION_NAME = "name";
    public static final String SERVICE_URL = "serviceUrl";
    public static final String AUTHENTICATION = "authentication";
    public static final String OPERATION_TIMEOUT_SECONDS = "operationTimeoutSeconds";
    public static final String STATS_INTERVAL_SECONDS = "statsIntervalSeconds";
    public static final String NUM_IO_THREADS = "numIoThreads";
    public static final String NUM_LISTENER_THREADS = "numListenerThreads";
    public static final String USE_TCP_NO_DELAY = "useTcpNoDelay";
    public static final String REQUEST_TIMEOUT_MS = "requestTimeoutMs";
    public static final String MAX_LOOKUP_REQUESTS = "maxLookupRequests";
    public static final String MAX_CONCURRENT_LOOKUP_REQUESTS = "maxConcurrentLookupRequests";
    public static final String MAX_REJECTED_REQUESTS_PER_CONNECTION = "maxRejectedRequestsPerConnection";
    public static final String KEEP_ALIVE_INTERVAL_SECONDS = "keepAliveIntervalSeconds";
    public static final String MAX_BACKOFF_INTERVAL_NANOS = "maxBackoffIntervalNanos";
    public static final String CONCURRENT_LOOKUP_REQUEST = "concurrentLookupRequest";
    public static final String CONNECTION_MAX_IDLE_SECONDS = "connectionMaxIdleSeconds";
    public static final String CONNECTION_TIMEOUT_MS = "connectionTimeoutMs";
    public static final String CONNECTIONS_PER_BROKER = "connectionsPerBroker";
    public static final String DESCRIPTION = "description";
    public static final String DNS_LOOKUP_BIND_ADDRESS = "dnsLookupBindAddress";
    public static final String DNS_LOOKUP_BIND_PORT = "dnsLookupBindPort";
    public static final String DNS_SERVER_ADDRESSES = "dnsServerAddresses";
    public static final String ENABLE_BUSY_WAIT = "enableBusyWait";
    public static final String ENABLE_TRANSACTION = "enableTransaction";
    public static final String INITIAL_BACKOFF_INTERVAL_NANOS = "initialBackoffIntervalNanos";
    public static final String LISTENER_NAME = "listenerName";
    public static final String LOOKUP_TIMEOUT_MS = "lookupTimeoutMs";
    public static final String MAX_LOOKUP_REDIRECTS = "maxLookupRedirects";
    public static final String MAX_LOOKUP_REQUEST = "maxLookupRequest";
    public static final String MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION = "maxNumberOfRejectedRequestPerConnection";
    public static final String MEMORY_LIMIT_BYTES = "memoryLimitBytes";

    public static final String TLS_CERTIFICATE_FILE_PATH = "tlsCertificateFilePath";
    public static final String TLS_KEY_FILE_PATH = "tlsKeyFilePath";
    public static final String TLS_TRUST_CERTS_FILE_PATH = "tlsTrustCertsFilePath";
    public static final String TLS_ALLOW_INSECURE_CONNECTION = "tlsAllowInsecureConnection";
    public static final String TLS_HOSTNAME_VERIFICATION_ENABLE = "tlsHostnameVerificationEnable";
    public static final String USE_KEY_STORE_TLS = "useKeyStoreTls";
    public static final String TLS_TRUST_STORE_TYPE = "tlsTrustStoreType";
    public static final String TLS_TRUST_STORE_PATH = "tlsTrustStorePath";
    public static final String TLS_TRUST_STORE_PASSWORD = "tlsTrustStorePassword";
    public static final String TLS_KEY_STORE_TYPE = "tlsKeyStoreType";
    public static final String TLS_KEY_STORE_PATH = "tlsKeyStorePath";
    public static final String TLS_KEY_STORE_PASSWORD = "tlsKeyStorePassword";
    public static final String TLS_CIPHERS = "tlsCiphers";
    public static final String TLS_PROTOCOLS = "tlsProtocols";
    public static final String USE_TLS = "useTLS";

    public static final String AUTH_TYPE = "authorizationType";
    public static final String AUTH_JWT = "JWT";
    public static final String AUTH_TLS = "TLS";
    public static final String AUTH_OAUTH2 = "OAUTH2";
    public static final String AUTH_NONE = "NONE";
    public static final String AUTH_PARAM_MAP = "authParamMap";
    public static final String AUTH_PARAMS = "authParams";
    public static final String AUTH_PLUGIN_CLASS_NAME = "authPluginClassName";
    public static final String AUTO_CERT_REFRESH_SECONDS = "autoCertRefreshSeconds";
    public static final String TOKEN = "token";

    public static final String TOPIC_NAMES = "topicNames";
    public static final String TOPICS_PATTERN = "topicsPattern";
    public static final String SUBSCRIPTION_NAME = "subscriptionName";
    public static final String SUBSCRIPTION_TYPE = "subscriptionType";
    public static final String SUBSCRIPTION_TOPICS_MODE = "subscriptionTopicsMode";
    public static final String SUBSCRIPTION_INITIAL_POSITION = "subscriptionInitialPosition";
    public static final String PROCESSING_MODE = "processingMode";


    public static final String ACK_TIMEOUT_MILLIS = "ackTimeoutMillis";
    public static final String NACK_REDELIVERY_DELAY = "nackRedeliveryDelay";
    public static final String RECEIVER_QUEUE_SIZE = "receiverQueueSize";
    public static final String MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS = "maxTotalReceiverQueueSizeAcrossPartitions";

    public static final String DLQ_TOPIC = "dlqTopic";
    public static final String DLQ_MAX_REDELIVERY_COUNT = "dlqMaxRedeliverCount";

    public static final String AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL = "autoAckOldestChunkedMessageOnQueueFull";
    public static final String MAX_PENDING_CHUNKED_MESSAGE = "maxPendingChunkedMessage";
    public static final String EXPIRY_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS = "expiryTimeOfIncompleteChunkedMessageMillis";

    public static final String AUTO_UPDATE_PARTITIONS = "autoUpdatePartitions";
    public static final String AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS = "autoUpdatePartitionsIntervalSeconds";
    public static final String REPLICATE_SUBSCRIPTION_STATE = "replicateSubscriptionState";
    public static final String READ_COMPACTED = "readCompacted";
    public static final String CRYPTO_FAILURE_ACTION = "cryptoFailureAction";


    public static final String CONSUMER_NAME = "consumerName";
    public static final String PRIORITY_LEVEL = "priorityLevel";

    public static final String BATCH_RECEIVE_ENABLED = "batchReceiveEnabled";
    public static final String BATCHING_MAX_MESSAGES = "batchingMaxMessages";
    public static final String BATCHING_MAX_BYTES = "batchingMaxBytes";
    public static final String BATCHING_TIMEOUT = "batchingTimeout";
    public static final String BATCH_INDEX_ACK_ENABLED = "batchIndexAckEnabled";


    public static final String CONTENT_TYPE = "contentType";

    public static final String SYNC = "SYNC";

    public static final String PROPERTIES = "properties";

}
