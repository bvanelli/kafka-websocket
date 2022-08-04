/*
    Copyright 2014 Benjamin Black

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

package us.b3k.kafka.ws;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.b3k.kafka.ws.consumer.KafkaConsumerImpl;
import us.b3k.kafka.ws.consumer.KafkaConsumerFactory;
import us.b3k.kafka.ws.messages.BinaryMessage;
import us.b3k.kafka.ws.messages.BinaryMessage.BinaryMessageDecoder;
import us.b3k.kafka.ws.messages.BinaryMessage.BinaryMessageEncoder;
import us.b3k.kafka.ws.messages.TextMessage;
import us.b3k.kafka.ws.messages.TextMessage.TextMessageDecoder;
import us.b3k.kafka.ws.messages.TextMessage.TextMessageEncoder;
import us.b3k.kafka.ws.producer.KafkaWebsocketProducer;

import org.keycloak.TokenVerifier;
import org.keycloak.common.VerificationException;
import org.keycloak.representations.AccessToken;


import javax.websocket.*;
import javax.websocket.server.HandshakeRequest;
import javax.websocket.server.ServerEndpoint;
import javax.websocket.server.ServerEndpointConfig;
import java.io.IOException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

@ServerEndpoint(
        value = "/v2/broker/",
        subprotocols = {"kafka-text", "kafka-binary"},
        decoders = {BinaryMessageDecoder.class, TextMessageDecoder.class},
        encoders = {BinaryMessageEncoder.class, TextMessageEncoder.class},
        configurator = KafkaWebsocketEndpoint.Configurator.class
)
public class KafkaWebsocketEndpoint {
    private static Logger LOG = LoggerFactory.getLogger(KafkaWebsocketEndpoint.class);

    private KafkaConsumerImpl consumer = null;

    public static Map<String, String> getQueryMap(String query) {
        Map<String, String> map = Maps.newHashMap();
        if (query != null) {
            String[] params = query.split("&");
            for (String param : params) {
                String[] nameval = param.split("=");
                map.put(nameval[0], nameval[1]);
            }
        }
        return map;
    }

    private KafkaWebsocketProducer producer() {
        return Configurator.PRODUCER;
    }

    @OnOpen
    @SuppressWarnings("unchecked")
    public void onOpen(final Session session) {
        String groupId = "";
        String topics = "";

        if (session.getUserProperties().get("kafka_principal") == null)
            this.closeSession(session, new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Unauthorized to open connection."));

        Map<String, String> queryParams = getQueryMap(session.getQueryString());
        if (queryParams.containsKey("group.id")) {
            groupId = queryParams.get("group.id");
        }

        LOG.debug("Opening new session {}", session.getId());
        if (queryParams.containsKey("topics")) {
            topics = queryParams.get("topics");
            LOG.debug("Session {} topics are {}", session.getId(), topics);
            consumer = Configurator.CONSUMER_FACTORY.getConsumer(groupId, topics, session);
        }
    }

    @OnClose
    public void onClose(final Session session) {
        if (consumer != null) {
            consumer.stop();
        }
    }

    @OnMessage
    public void onMessage(final BinaryMessage message, final Session session) {
        LOG.trace("Received binary message: topic - {}; message - {}",
                message.getTopic(), message.getMessage());
        producer().send(message, session);
    }

    @OnMessage
    public void onMessage(final TextMessage message, final Session session) {
        LOG.trace("Received text message: topic - {}; key - {}; message - {}",
                message.getTopic(), message.getKey(), message.getMessage());
        String principal = session.getUserProperties().get("kafka_principal").toString();
        String topic = principal + "-" + message.getTopic();
        message.setTopic(topic);
        producer().send(message, session);
    }

    private void closeSession(Session session, CloseReason reason) {
        try {
            session.close(reason);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Configurator extends ServerEndpointConfig.Configurator {
        public static KafkaConsumerFactory CONSUMER_FACTORY;
        public static KafkaWebsocketProducer PRODUCER;

        public static PublicKey publicKey;

        public static String authorization_header = "Authorization";

        public static void setPublicKey(String publicKeyString) throws NoSuchAlgorithmException, InvalidKeySpecException {
            byte[] publicBytes = Base64.getDecoder().decode(publicKeyString);
            X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            publicKey = keyFactory.generatePublic(keySpec);
        }

        @Override
        public void modifyHandshake(ServerEndpointConfig config, HandshakeRequest request, HandshakeResponse response) {
            super.modifyHandshake(config, request, response);
            List<String> bearer = request.getHeaders().get(authorization_header);

            if (bearer == null) {
                LOG.debug("Bearer is null, rejecting connection");
            } else {
                LOG.debug("Received bearer" + bearer);
                try {
                    String bearer_token = bearer.get(0).split(" ")[1];
                    TokenVerifier<AccessToken> verifier = TokenVerifier.create(bearer_token, AccessToken.class).withChecks(TokenVerifier.IS_ACTIVE);
                    AccessToken token = verifier.publicKey(publicKey).verify().getToken();
                    String username = token.getPreferredUsername();
                    LOG.debug("Validated token for user " + username);
                    config.getUserProperties().put("kafka_principal", username);
                    return;
                } catch (VerificationException | IndexOutOfBoundsException e) {
                }
            }
            response.getHeaders().put(HandshakeResponse.SEC_WEBSOCKET_ACCEPT, new ArrayList<>());
        }

        @Override
        public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
            T endpoint = super.getEndpointInstance(endpointClass);

            if (endpoint instanceof KafkaWebsocketEndpoint) {
                return endpoint;
            }
            throw new InstantiationException(
                    MessageFormat.format("Expected instanceof \"{0}\". Got instanceof \"{1}\".",
                            KafkaWebsocketEndpoint.class, endpoint.getClass()));
        }
    }
}

