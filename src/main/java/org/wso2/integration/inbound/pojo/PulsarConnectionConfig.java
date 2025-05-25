package org.wso2.integration.inbound.pojo;

import org.apache.commons.lang.StringUtils;
import org.apache.synapse.SynapseException;

public class PulsarConnectionConfig {

    private String serviceUrl;
    private Integer operationTimeoutSeconds;
    private Integer statsIntervalSeconds;
    private Integer numIoThreads;
    private Integer numListenerThreads;
    private Boolean useTcpNoDelay;
    private Integer requestTimeoutMs;
    private Integer maxLookupRequest;
    private Integer keepAliveIntervalSeconds;
    private Long maxBackoffIntervalNanos;
    private Integer concurrentLookupRequest;
    private Integer connectionMaxIdleSeconds;
    private Integer connectionTimeoutMs;
    private Integer connectionsPerBroker;
    private Boolean enableBusyWait;
    private Boolean enableTransaction;
    private Long initialBackoffIntervalNanos;
    private String listenerName;
    private Integer lookupTimeoutMs;
    private Integer maxLookupRedirects;
    private Integer maxNumberOfRejectedRequestPerConnection;
    private Long memoryLimitBytes;

    public String getServiceUrl() {

        return serviceUrl;
    }

    public void setServiceUrl(String serviceUrl) throws SynapseException {

        if (StringUtils.isNotEmpty(serviceUrl)) {
            this.serviceUrl = serviceUrl;
        } else {
            throw new SynapseException("Mandatory parameter 'serviceUrl' is not set.");
        }
    }

    public Integer getOperationTimeoutSeconds() {

        return operationTimeoutSeconds;
    }

    public void setOperationTimeoutSeconds(String operationTimeoutSeconds) {

        if (StringUtils.isNotEmpty(operationTimeoutSeconds)) {
            this.operationTimeoutSeconds = Integer.parseInt(operationTimeoutSeconds);
        }
    }

    public Integer getStatsIntervalSeconds() {

        return statsIntervalSeconds;
    }

    public void setStatsIntervalSeconds(String statsIntervalSeconds) {

        if (StringUtils.isNotEmpty(statsIntervalSeconds)) {
            this.statsIntervalSeconds = Integer.parseInt(statsIntervalSeconds);
        }
    }

    public Integer getNumIoThreads() {

        return numIoThreads;
    }

    public void setNumIoThreads(String numIoThreads) {

        if (StringUtils.isNotEmpty(numIoThreads)) {
            this.numIoThreads = Integer.parseInt(numIoThreads);
        }
    }

    public Integer getNumListenerThreads() {

        return numListenerThreads;
    }

    public void setNumListenerThreads(String numListenerThreads) {

        if (StringUtils.isNotEmpty(numListenerThreads)) {
            this.numListenerThreads = Integer.parseInt(numListenerThreads);
        }
    }

    public Boolean getUseTcpNoDelay() {

        return useTcpNoDelay;
    }

    public void setUseTcpNoDelay(String useTcpNoDelay) {

        if (StringUtils.isNotEmpty(useTcpNoDelay)) {
            this.useTcpNoDelay = Boolean.parseBoolean(useTcpNoDelay);
        }
    }

    public Integer getRequestTimeoutMs() {

        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(String requestTimeoutMs) {

        if (StringUtils.isNotEmpty(requestTimeoutMs)) {
            this.requestTimeoutMs = Integer.parseInt(requestTimeoutMs);
        }
    }

    public Integer getKeepAliveIntervalSeconds() {

        return keepAliveIntervalSeconds;
    }

    public void setKeepAliveIntervalSeconds(String keepAliveIntervalSeconds) {

        if (StringUtils.isNotEmpty(keepAliveIntervalSeconds)) {
            this.keepAliveIntervalSeconds = Integer.parseInt(keepAliveIntervalSeconds);
        }
    }

    public Long getMaxBackoffIntervalNanos() {

        return maxBackoffIntervalNanos;
    }

    public void setMaxBackoffIntervalNanos(String maxBackoffIntervalNanos) {

        if (StringUtils.isNotEmpty(maxBackoffIntervalNanos)) {
            this.maxBackoffIntervalNanos = Long.parseLong(maxBackoffIntervalNanos);
        }
    }

    public Integer getConcurrentLookupRequest() {

        return concurrentLookupRequest;
    }

    public void setConcurrentLookupRequest(String concurrentLookupRequest) {

        if (StringUtils.isNotEmpty(concurrentLookupRequest)) {
            this.concurrentLookupRequest = Integer.parseInt(concurrentLookupRequest);
        }
    }

    public Integer getConnectionMaxIdleSeconds() {

        return connectionMaxIdleSeconds;
    }

    public void setConnectionMaxIdleSeconds(String connectionMaxIdleSeconds) {

        if (StringUtils.isNotEmpty(connectionMaxIdleSeconds)) {
            this.connectionMaxIdleSeconds = Integer.parseInt(connectionMaxIdleSeconds);
        }
    }

    public Integer getConnectionTimeoutMs() {

        return connectionTimeoutMs;
    }

    public void setConnectionTimeoutMs(String connectionTimeoutMs) {

        if (StringUtils.isNotEmpty(connectionTimeoutMs)) {
            this.connectionTimeoutMs = Integer.parseInt(connectionTimeoutMs);
        }
    }

    public Integer getConnectionsPerBroker() {

        return connectionsPerBroker;
    }

    public void setConnectionsPerBroker(String connectionsPerBroker) {

        if (StringUtils.isNotEmpty(connectionsPerBroker)) {
            this.connectionsPerBroker = Integer.parseInt(connectionsPerBroker);
        }
    }

    public Boolean getEnableBusyWait() {

        return enableBusyWait;
    }

    public void setEnableBusyWait(String enableBusyWait) {

        if (StringUtils.isNotEmpty(enableBusyWait)) {
            this.enableBusyWait = Boolean.parseBoolean(enableBusyWait);
        }
    }

    public Boolean getEnableTransaction() {

        return enableTransaction;
    }

    public void setEnableTransaction(String enableTransaction) {

        if (StringUtils.isNotEmpty(enableTransaction)) {
            this.enableTransaction = Boolean.parseBoolean(enableTransaction);
        }
    }

    public Long getInitialBackoffIntervalNanos() {

        return initialBackoffIntervalNanos;
    }

    public void setInitialBackoffIntervalNanos(String initialBackoffIntervalNanos) {

        if (StringUtils.isNotEmpty(initialBackoffIntervalNanos)) {
            this.initialBackoffIntervalNanos = Long.parseLong(initialBackoffIntervalNanos);
        }
    }

    public String getListenerName() {

        return listenerName;

    }

    public void setListenerName(String listenerName) {

        if (StringUtils.isNotEmpty(listenerName)) {
            this.listenerName = listenerName;
        }
    }

    public Integer getLookupTimeoutMs() {

        return lookupTimeoutMs;
    }

    public void setLookupTimeoutMs(String lookupTimeoutMs) {

        if (StringUtils.isNotEmpty(lookupTimeoutMs)) {
            this.lookupTimeoutMs = Integer.parseInt(lookupTimeoutMs);
        }
    }

    public Integer getMaxLookupRedirects() {

        return maxLookupRedirects;
    }

    public void setMaxLookupRedirects(String maxLookupRedirects) {

        if (StringUtils.isNotEmpty(maxLookupRedirects)) {
            this.maxLookupRedirects = Integer.parseInt(maxLookupRedirects);
        }
    }

    public Integer getMaxLookupRequest() {

        return maxLookupRequest;
    }

    public void setMaxLookupRequest(String maxLookupRequest) {

        if (StringUtils.isNotEmpty(maxLookupRequest)) {
            this.maxLookupRequest = Integer.parseInt(maxLookupRequest);
        }
    }

    public Integer getMaxNumberOfRejectedRequestPerConnection() {

        return maxNumberOfRejectedRequestPerConnection;
    }

    public void setMaxNumberOfRejectedRequestPerConnection(String maxNumberOfRejectedRequestPerConnection) {

        if (StringUtils.isNotEmpty(maxNumberOfRejectedRequestPerConnection)) {
            this.maxNumberOfRejectedRequestPerConnection = Integer.parseInt(maxNumberOfRejectedRequestPerConnection);
        }
    }

    public Long getMemoryLimitBytes() {

        return memoryLimitBytes;
    }

    public void setMemoryLimitBytes(String memoryLimitBytes) {

        if (StringUtils.isNotEmpty(memoryLimitBytes)) {
            this.memoryLimitBytes = Long.parseLong(memoryLimitBytes);
        }
    }
}
