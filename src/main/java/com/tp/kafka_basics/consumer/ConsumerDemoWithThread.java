package com.tp.kafka_basics.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {

    /*
        There is no new concept in this program.Actually in this program we will see how to write kafka program to
        swiftly exit from the program. In previous programs you had seen that the program continues to run until we
        manually exit from the program and that is right also as consumer should continuously run, so it can read
        all upcoming messages.But the problem is when we exit from the program.In that case we are not even closing
        the consumer.So that is not the proper way of dealing with kafka consumer.In this part we will see a proper way
        of how to use concept of threads to deal with kafka consumer.
     */
    public static void main(String[] args) {
        new ConsumerDemoWithThread().consumeMessages();
    }

    public void consumeMessages(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServers = "192.168.56.13:9092";
        String groupId = "my-new-consumer-group";
        String topic = "first-topic";
        CountDownLatch latch = new CountDownLatch(1);
        //Creating consumer runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, topic);
        //Starting a new thread with consumer runnable
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        // Will be called when we manually exit from the program or shut down the application.
        // i.e. when we press on red square button in intellij to stop the program as until that the program will
        // continue to run.
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try{
                latch.await();
            } catch (InterruptedException e){
                logger.error("Got error in exiting from application", e);
            }
            logger.info("Application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e){
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }


class ConsumerRunnable extends Thread {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);

    ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String topic){
        this.latch = latch;
        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Creating kafka consumer
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Key :" + record.key() + "\n" +
                            "Value :" + record.value() + "\n" +
                            "Partition :" + record.partition() + "\n" +
                            "Offset :" + record.offset() + "\n"
                    );
                }
            }
        } catch (WakeupException e){
            logger.info("Received shutdown signal.");
        } finally {
            consumer.close();
            // It will tell our main code that ConsumerRunnable task is completed. Actually latch.await() waits until
            // The latch count becomes 0. While creating latch in main() method of ConsumerDemoWithThread class we
            // passed argument as 1 i.e. we set the latch count as 1 there. Now after calling latch.countDown()
            // latch count will decrement by 1 and becomes 0. So latch.await() in main method will terminate its
            // waiting and will proceed further.
            latch.countDown();
        }
    }

    public void shutdown(){
        //It is a special method to interrupt consumer.poll method
        //It throws an exception WakeUpException
        consumer.wakeup();
    }
}


}