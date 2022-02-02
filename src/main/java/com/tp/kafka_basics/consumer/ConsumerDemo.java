package com.tp.kafka_basics.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServers = "192.168.56.13:9092";
        String groupId = "my-kafka-consumer-group";
        String topic = "first-topic";

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /*
            Here "earliest" means read the messages from the beginning i.e. those messages also which were published to
            topic even before the consumer subscribe to the topic.Other options are "latest" and "none".
            "latest" meaning read only those messages which will be now published to the topic and discard all previous
            messages. "none" is very rarely used. It throws error if no message is currently being published at the time
            of subscribing any topic.
         */

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe consumer to our topic
        consumer.subscribe(Collections.singleton(topic));
        //But what if you want to subscribe multiple topics.You can do it as follows:
        //consumer.subscribe(Arrays.asList("first-topic", "second-topic", "third-topic"));



        // Poll for new data
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records ) {
                logger.info("Key :"+record.key()+"\n"+
                            "Value :"+record.value()+"\n"+
                            "Partition :"+record.partition()+"\n"+
                            "Offset :"+record.offset()+"\n"
                        );
            }
        }
    }
}
