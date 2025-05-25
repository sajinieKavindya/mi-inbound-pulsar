package org.wso2.integration.inbound.pojo;

public class ConnectionConfiguration {

    private String connectionName;
    private PulsarConnectionConfig connectionConfig;
    private PulsarAuthConfig authConfig;
    private Boolean useTlsEncryption;

    public String getConnectionName() {

        return connectionName;
    }

    public void setConnectionName(String connectionName) {

        this.connectionName = connectionName;
    }

    public Boolean getUseTlsEncryption() {

        return useTlsEncryption;
    }

    public void setUseTlsEncryption(String useTlsEncryption, String serviceUrl) {

        if (serviceUrl.startsWith("pulsar+ssl://") || serviceUrl.startsWith("https")) {
            this.useTlsEncryption = true;
        } else if (serviceUrl.startsWith("pulsar://")) {
            this.useTlsEncryption = false;
        } else {
            this.useTlsEncryption = Boolean.parseBoolean(useTlsEncryption);
        }
    }

    public PulsarConnectionConfig getConnectionConfig() {

        return connectionConfig;
    }

    public void setConnectionConfig(PulsarConnectionConfig connectionConfig) {

        this.connectionConfig = connectionConfig;
    }

    public PulsarAuthConfig getAuthConfig() {

        return authConfig;
    }

    public void setAuthConfig(PulsarAuthConfig authConfig) {

        this.authConfig = authConfig;
    }
}
