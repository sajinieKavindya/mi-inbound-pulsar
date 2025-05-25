package org.wso2.integration.inbound.connection;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.synapse.MessageContext;
import org.wso2.integration.inbound.pojo.ConnectionConfiguration;

public class PulsarConnection {

    private static final String ERROR_MESSAGE = "Error occurred while connecting to the Pulsar server.";

    private PulsarClient client;

    public PulsarConnection(MessageContext messageContext, ConnectionConfiguration configuration) throws Exception {

        try {
            ClientBuilder clientBuilder = PulsarClient.builder();
            PulsarConnectionSetup connectionSetup = new PulsarConnectionSetup();
            connectionSetup.constructClientBuilder(configuration, clientBuilder);
            this.client = clientBuilder.build();

        } catch (IllegalArgumentException e) {
//            PulsarUtils.handleError(messageContext, e, 700000, ERROR_MESSAGE);
        } catch (PulsarClientException e) {
//            PulsarUtils.handleError(messageContext, e, 908989, ERROR_MESSAGE);
        } catch (Exception e) {
//            PulsarUtils.handleError(messageContext, e, 900000, ERROR_MESSAGE);
        }
    }

    public PulsarClient getClient() {

        return client;
    }
}
