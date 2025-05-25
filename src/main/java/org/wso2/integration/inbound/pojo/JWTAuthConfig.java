package org.wso2.integration.inbound.pojo;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;

public class JWTAuthConfig extends PulsarAuthConfig {

    private String token;

    @Override
    public String getAuthMethod() {
        return "JWT";
    }

    public String getToken() {

        return token;
    }

    public void setToken(String token) {

        this.token = token;
    }

    @Override
    public Authentication getAuthentication() {

        return AuthenticationFactory.token(token);
    }

}
