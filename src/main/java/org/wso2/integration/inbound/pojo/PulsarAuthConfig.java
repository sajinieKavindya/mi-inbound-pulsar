package org.wso2.integration.inbound.pojo;

import org.apache.pulsar.client.api.Authentication;

public abstract class PulsarAuthConfig extends PulsarConnectionConfig {

    private String authentication;
    private String authParams;
    protected String authPluginClassName;


    public abstract String getAuthMethod();

    public abstract Authentication getAuthentication();

}
