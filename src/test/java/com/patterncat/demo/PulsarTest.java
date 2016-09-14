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
        ConsumerConfiguration consumerConfig = new ConsumerConfiguration();
        consumerConfig.setReceiverQueueSize(10);
        Consumer consumer = client.subscribe(
                "persistent://sample/standalone/ns1/my-topic",
                "my-subscribtion-name2",consumerConfig);

//        for(int i=0;i<490;i++) {
//            // Wait for a message
//            Message msg = consumer.receive();
//
//            System.out.println("Received message: " + new String(msg.getData()));
//
//            // Acknowledge the message so that it can be deleted by broker
//            consumer.acknowledge(msg);
//        }

        System.out.println("debug flow command");

        for(int i=0;i<10;i++){
            Message msg = consumer.receive();
            System.out.println("Received message: " + new String(msg.getData()));
        }

        client.close();
    }

    @Test
    public void producer() throws PulsarClientException {
        PulsarClient client = PulsarClient.create("http://localhost:8080");

        Producer producer = client.createProducer(
                "persistent://sample/standalone/ns1/my-topic");

// Publish 10 messages to the topic
        for (int i = 1; i <= 2020; i++) {
            String msg = "my-message"+i;
            producer.send(msg.getBytes());
        }

        client.close();
    }
}
