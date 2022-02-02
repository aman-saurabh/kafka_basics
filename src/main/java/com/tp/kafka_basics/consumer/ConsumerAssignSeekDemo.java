package com.tp.kafka_basics.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerAssignSeekDemo {
    /*
        Concept of Assign and Seek is used in kafka if we want to read few particular messages of a particular
        topic and not actually want to subscribe the topic.
        But we know that messages of a topic is distributed in its partitions and every partition maintains its own
        offset. So to retrieve any particular message of a topic we need partition number of the topic and offset of
        the message in that partition.For example, we want to retrieve 5 message of partition0 whose offset in
        partition0 is 15 as follows:
     */
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServers = "192.168.56.13:9092";
        String topic = "first-topic";
        // GroupId is not required here as we are not going to subscribe any topic here.
        // We will read just1 message and consumer will be closed instantly.

        // Create consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //Assign
        TopicPartition partitionNum = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionNum));

        //Seek
        long offsetToReadFrom = 15L;
        consumer.seek(partitionNum, offsetToReadFrom);

        //
        int totalMessagesToRead = 5;
        int messagesReadSoFar = 0;

        while (messagesReadSoFar<=totalMessagesToRead){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records ) {
                logger.info("Key :"+record.key()+"\n"+
                        "Value :"+record.value()+"\n"+
                        "Partition :"+record.partition()+"\n"+
                        "Offset :"+record.offset()+"\n"
                );
                messagesReadSoFar++;
                if (messagesReadSoFar >= totalMessagesToRead){
                    break;
                }
            }
        }

        logger.info("Exiting the application.");
    }
}
