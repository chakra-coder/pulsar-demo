package com.patterncat.demo;

import com.github.rholder.retry.*;
import com.google.common.base.Predicates;
import com.yahoo.pulsar.client.api.*;
import org.junit.Test;

import java.util.concurrent.*;

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
            // Acknowledge the message so that it can be deleted by broker
            consumer.acknowledge(msg);
        }

        client.close();
    }

    @Test
    public void producer() throws PulsarClientException, InterruptedException {
        try{
            PulsarClient client = PulsarClient.create("http://localhost:8080");
            Producer producer = client.createProducer(
                    "persistent://sample/standalone/ns1/my-topic");
//
// Publish 10 messages to the topic
            for (int i = 0; i < 10; i++) {
                producer.send("my-message".getBytes());
            }

            client.close();
        }catch (Exception e){
            e.printStackTrace();
            Thread.sleep(100*1000L);
        }


    }

    @Test
    public void producerWithRetry() throws PulsarClientException {
        PulsarClient client = PulsarClient.create("http://localhost:8080");
        Callable<Producer> callable = new Callable<Producer>() {
            public Producer call() throws Exception {
                System.out.println("try to connect to pulsar borker");
                Producer producer = client.createProducer(
                        "persistent://sample/standalone/ns1/my-topic");
                return producer;
            }
        };

        Retryer<Producer> retryer = RetryerBuilder.<Producer>newBuilder()
                .retryIfResult(Predicates.<Producer>isNull())
                .retryIfExceptionOfType(PulsarClientException.class)
                .retryIfRuntimeException()
                .withWaitStrategy(WaitStrategies.fixedWait(1,TimeUnit.SECONDS))
                .withStopStrategy(StopStrategies.neverStop())
                .build();
        try {
            Producer producer = retryer.call(callable);
            System.out.println("producer="+producer);
            for (int i = 0; i < 10000; i++) {
                try{
                    producer.send("my-message".getBytes());
                    System.out.println("sended"+i);
                    Thread.sleep(1000);
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        } catch (RetryException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
