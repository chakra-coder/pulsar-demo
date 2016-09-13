package com.patterncat.demo;

import com.yahoo.pulsar.client.api.*;
import org.junit.Test;

/**
 * Created by patterncat on 2016-09-13.
 */
public class PulsarTest {

    @Test
    public void consumer() throws PulsarClientException {
        PulsarClient client = PulsarClient.create("http://localhost:8080");

        Consumer consumer = client.subscribe(
                "persistent://sample/standalone/ns1/my-topic",
                "my-subscribtion-name");

        for(int i=0;i<100;i++) {
            // Wait for a message
            Message msg = consumer.receive();

            System.out.println("Received message: " + new String(msg.getData()));

            // Acknowledge the message so that it can be deleted by broker
            consumer.acknowledge(msg);
        }

        client.close();
    }

    @Test
    public void producer() throws PulsarClientException {
        PulsarClient client = PulsarClient.create("http://localhost:8080");

        Producer producer = client.createProducer(
                "persistent://sample/standalone/ns1/my-topic");

// Publish 10 messages to the topic
        for (int i = 0; i < 10; i++) {
            producer.send("my-message".getBytes());
        }

        client.close();
    }
}
