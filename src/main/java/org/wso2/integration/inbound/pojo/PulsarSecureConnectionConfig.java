package org.wso2.integration.inbound.pojo;


import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class PulsarSecureConnectionConfig extends PulsarConnectionConfig {

    private Boolean useTls;
    private Boolean tlsAllowInsecureConnection;
    private Boolean enableTlsHostnameVerification;
    private String tlsTrustCertsFilePath;
    private Set<String> tlsProtocols;
    private Set<String> tlsCiphers;
    private Boolean useKeyStoreTls;
    private String tlsTrustStorePath;
    private String tlsTrustStorePassword;
    private String tlsTrustStoreType;
    private Integer autoCertRefreshSeconds;

//    public ClientBuilder constructClientBuilder(ClientBuilder builder) {
//        ClientBuilder clientBuilder = super.constructClientBuilder(builder);
//
//        if (useTls != null) {
//            clientBuilder.enableTls(useTls);
//        }
//        if (tlsAllowInsecureConnection != null) {
//            clientBuilder.allowTlsInsecureConnection(tlsAllowInsecureConnection);
//        }
//        if (enableTlsHostnameVerification != null) {
//            clientBuilder.enableTlsHostnameVerification(enableTlsHostnameVerification);
//        }
//        if (tlsTrustCertsFilePath != null) {
//            clientBuilder.tlsTrustCertsFilePath(tlsTrustCertsFilePath);
//        }
//        if (tlsProtocols != null) {
//            clientBuilder.tlsProtocols(tlsProtocols);
//        }
//        if (tlsCiphers != null) {
//            clientBuilder.tlsCiphers(tlsCiphers);
//        }
//        if (useKeyStoreTls != null) {
//            clientBuilder.useKeyStoreTls(useKeyStoreTls);
//        }
//        if (tlsTrustStorePath != null) {
//            clientBuilder.tlsTrustStorePath(tlsTrustStorePath);
//        }
//        if (tlsTrustStorePassword != null) {
//            clientBuilder.tlsTrustStorePassword(tlsTrustStorePassword);
//        }
//        if (tlsTrustStoreType != null) {
//            clientBuilder.tlsTrustStoreType(tlsTrustStoreType);
//        }
//        if (autoCertRefreshSeconds != null) {
//            clientBuilder.autoCertRefreshSeconds(autoCertRefreshSeconds);
//        }
//
////        loadConfig(PulsarConstants.USE_TLS, useTls, connectionConfig);
////        loadConfig(PulsarConstants.TLS_ALLOW_INSECURE_CONNECTION, tlsAllowInsecureConnection, connectionConfig);
////        loadConfig(PulsarConstants.TLS_HOSTNAME_VERIFICATION_ENABLE, enableTlsHostnameVerification, connectionConfig);
////        loadConfig(PulsarConstants.TLS_TRUST_CERTS_FILE_PATH, tlsTrustCertsFilePath, connectionConfig);
////        loadConfig(PulsarConstants.USE_KEY_STORE_TLS, useKeyStoreTls, connectionConfig);
////        loadConfig(PulsarConstants.TLS_TRUST_STORE_PATH, tlsTrustStorePath, connectionConfig);
////        loadConfig(PulsarConstants.TLS_TRUST_STORE_PASSWORD, tlsTrustStorePassword, connectionConfig);
////        loadConfig(PulsarConstants.TLS_TRUST_STORE_TYPE, tlsTrustStoreType, connectionConfig);
////        loadConfig(PulsarConstants.AUTO_CERT_REFRESH_SECONDS, autoCertRefreshSeconds, connectionConfig);
//        return clientBuilder;
//    }

    public Boolean useTls() {
        return useTls;
    }

    public void setUseTls(String useTls) {
        if (StringUtils.isNotEmpty(useTls)) {
            this.useTls = Boolean.parseBoolean(useTls);
        }
    }

    public Boolean getTlsAllowInsecureConnection() {
        return tlsAllowInsecureConnection;
    }

    public void setTlsAllowInsecureConnection(String tlsAllowInsecureConnection) {
        if (StringUtils.isNotEmpty(tlsAllowInsecureConnection)) {
            this.tlsAllowInsecureConnection = Boolean.parseBoolean(tlsAllowInsecureConnection);
        }
    }

    public Boolean getEnableTlsHostnameVerification() {
        return enableTlsHostnameVerification;
    }

    public void setEnableTlsHostnameVerification(String enableTlsHostnameVerification) {
        if (StringUtils.isNotEmpty(enableTlsHostnameVerification)) {
            this.enableTlsHostnameVerification = Boolean.parseBoolean(enableTlsHostnameVerification);
        }
    }

    public String getTlsTrustCertsFilePath() {
        return tlsTrustCertsFilePath;
    }

    public void setTlsTrustCertsFilePath(String tlsTrustCertsFilePath) {
        if (StringUtils.isNotEmpty(tlsTrustCertsFilePath)) {
            this.tlsTrustCertsFilePath = tlsTrustCertsFilePath;
        }
    }

    public Set<String> getTlsProtocols() {
        return tlsProtocols;
    }

    public void setTlsProtocols(String tlsProtocols) {
        if (StringUtils.isNotEmpty(tlsProtocols)) {
            this.tlsProtocols = new java.util.HashSet<>(java.util.Arrays.asList(tlsProtocols.split(",")));
        } else {
            this.tlsProtocols = java.util.Collections.emptySet();
        }
    }

    public Set<String> getTlsCiphers() {
        return tlsCiphers;
    }

    public void setTlsCiphers(String tlsCiphers) {
        if (StringUtils.isNotEmpty(tlsCiphers)) {
            this.tlsCiphers = new java.util.HashSet<>(java.util.Arrays.asList(tlsCiphers.split(",")));
        } else {
            this.tlsCiphers = java.util.Collections.emptySet();
        }
    }

    public Boolean getUseKeyStoreTls() {
        return useKeyStoreTls;
    }

    public void setUseKeyStoreTls(String useKeyStoreTls) {
        if (StringUtils.isNotEmpty(useKeyStoreTls)) {
            this.useKeyStoreTls = Boolean.parseBoolean(useKeyStoreTls);
        }
    }

    public String getTlsTrustStorePath() {
        return tlsTrustStorePath;
    }

    public void setTlsTrustStorePath(String tlsTrustStorePath) {
        if (StringUtils.isNotEmpty(tlsTrustStorePath)) {
            this.tlsTrustStorePath = tlsTrustStorePath;
        }
    }

    public String getTlsTrustStorePassword() {
        return tlsTrustStorePassword;
    }

    public void setTlsTrustStorePassword(String tlsTrustStorePassword) {
        if (StringUtils.isNotEmpty(tlsTrustStorePassword)) {
            this.tlsTrustStorePassword = tlsTrustStorePassword;
        }
    }

    public String getTlsTrustStoreType() {
        return tlsTrustStoreType;
    }

    public void setTlsTrustStoreType(String tlsTrustStoreType) {
        if (StringUtils.isNotEmpty(tlsTrustStoreType)) {
            this.tlsTrustStoreType = tlsTrustStoreType;
        }
    }

    public Integer getAutoCertRefreshSeconds() {
        return autoCertRefreshSeconds;
    }

    public void setAutoCertRefreshSeconds(String autoCertRefreshSeconds) {
        if (StringUtils.isNotEmpty(autoCertRefreshSeconds)) {
            this.autoCertRefreshSeconds = Integer.parseInt(autoCertRefreshSeconds);
        }
    }

}
