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

package us.b3k.kafka.ws.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.b3k.kafka.ws.messages.AbstractMessage;
import us.b3k.kafka.ws.messages.BinaryMessage;
import us.b3k.kafka.ws.messages.TextMessage;
import us.b3k.kafka.ws.transforms.Transform;

import javax.websocket.CloseReason;
import javax.websocket.RemoteEndpoint.Async;
import javax.websocket.Session;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;

public class KafkaConsumerImpl {
    private static Logger LOG = LoggerFactory.getLogger(KafkaConsumerImpl.class);

    private final ExecutorService executorService;
    private final Transform transform;
    private final Session session;
    private final ConsumerConfig consumerConfig;
    private KafkaConsumer<String, String> connector;
    private final List<String> topics;
    private final Async remoteEndpoint;

    public KafkaConsumerImpl(Properties configProps, final ExecutorService executorService, final Transform transform, final String topics, final Session session) {
        this.remoteEndpoint = session.getAsyncRemote();
        this.consumerConfig = new ConsumerConfig(configProps);
        this.executorService = executorService;
        this.topics = Arrays.asList(topics.split(","));
        this.transform = transform;
        this.session = session;
    }

    public KafkaConsumerImpl(ConsumerConfig consumerConfig, final ExecutorService executorService, final Transform transform, final List<String> topics, final Session session) {
        this.remoteEndpoint = session.getAsyncRemote();
        this.consumerConfig = consumerConfig;
        this.executorService = executorService;
        this.topics = topics;
        this.transform = transform;
        this.session = session;
    }

    public void start() {
        LOG.debug("Starting consumer for {}", session.getId());
        this.connector = new KafkaConsumer<>(consumerConfig.originals());

        connector.subscribe(topics);
        executorService.submit(new KafkaConsumerTask(connector, remoteEndpoint, transform, session));
    }

    public void stop() {
        LOG.info("Stopping consumer for session {}", session.getId());
        if (connector != null) {
            connector.commitSync();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ie) {
                LOG.error("Exception while waiting to shutdown consumer: {}", ie.getMessage());
            }
            LOG.debug("Shutting down connector for session {}", session.getId());
            connector.close();
        }
        LOG.info("Stopped consumer for session {}", session.getId());
    }

    static public class KafkaConsumerTask implements Runnable {
        private KafkaConsumer<String, String> stream;
        private Async remoteEndpoint;
        private final Transform transform;
        private final Session session;

        public KafkaConsumerTask(KafkaConsumer<String, String> stream, Async remoteEndpoint,
                                 final Transform transform, final Session session) {
            this.stream = stream;
            this.remoteEndpoint = remoteEndpoint;
            this.transform = transform;
            this.session = session;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            String subprotocol = session.getNegotiatedSubprotocol();

            while (true) {
                ConsumerRecords<String, String> records = stream.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (ConsumerRecord<String, String> record : records) {
                    String topic = record.topic();
                    String message = record.value();
                    switch(subprotocol) {
                        case "kafka-binary":
                            sendBinary(topic, message);
                            break;
                        default:
                            sendText(topic, message);
                            break;
                    }
                    if (Thread.currentThread().isInterrupted()) {
                        try {
                            session.close();
                        } catch (IOException e) {
                            LOG.error("Error terminating session: {}", e.getMessage());
                        }
                        return;
                    }
                }
            }
        }

        private void sendBinary(String topic, String message) {
            AbstractMessage msg = transform.transform(new BinaryMessage(topic, message.getBytes(StandardCharsets.UTF_8)), session);
            if(!msg.isDiscard()) {
                remoteEndpoint.sendObject(msg);
            }
        }

        private void sendText(String topic, String message) {
            LOG.trace("XXX Sending text message to remote endpoint: {} {}", topic, message);
            AbstractMessage msg = transform.transform(new TextMessage(topic, message), session);
            if(!msg.isDiscard()) {
                remoteEndpoint.sendObject(msg);
            }
        }

        private void closeSession(Exception e) {
            LOG.debug("Consumer initiated close of session {}", session.getId());
            try {
                session.close(new CloseReason(CloseReason.CloseCodes.CLOSED_ABNORMALLY, e.getMessage()));
            } catch (IOException ioe) {
                LOG.error("Error closing session: {}", ioe.getMessage());
            }
        }
    }
}
