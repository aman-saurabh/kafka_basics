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

public class ConsumerGroupDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServers = "192.168.56.13:9092";
        String groupId = "my-kafka-consumer-group";
        String topic = "first-topic";
        /*
            ****************************************************************************************************
            Note :-
            1.) The code on this file is exactly same as the code in ConsumerDemo file.But if you run this
            program after running the ConsumerDemo program you will find it didn't read any message even though
            we have set AUTO_OFFSET_RESET_CONFIG as "earliest".So it should have read message from the beginning,
            but it didn't do so. It is because since groupId(representing consumer-group) in ConsumerDemo file and
            this file is same, so when we read messages in ConsumerDemo file the offset is reset to the last read
            message. So when we again try to read the message in this file(i.e. with same consumer groupId) kafka
            search for messages after the offset point. But since no new messages were published after that, so it
            didn't read any message.If you change the groupId to something else then it will read messages from the
            beginning but only once after that for that consumer group also offset will be reset to current message
            and hence in next try it will not read any message.
            2.) In theory part we have read that if our kafka cluster contains multiple brokers then messages published
            to the topic is distributed in different partitions.For example if our kafka cluster consist of 3 brokers
            then each topic will have 3 partitions and if we publish 10 messages to such topic then the partition1 may
            get 4 messages and partition2 and partition3 may get 3 messages each. Here we are considering messages were
            published without keys. If keys were used then all messages with same key will go to same partition. Now
            come to the role of consumer group. A consumer group can have multiple consumers. For example - our
            consumer-group "my-kafka-consumer-group" have two consumers - One in ConsumerDemo file and another in this
            file.When a consumer group subscribe to a topic, consumers of the consumer group is assigned few particular
            partitions from where they can read message. Here note that any consumer of the consumer group can be
            assigned more than one partition, but it is not possible that any partition is assigned to multiple
            consumers of the same consumer group. So in any case only one consumer of the consumer group have exclusive
            right to read message from a single partition and no two consumers of a consumer group can read messages
            from the same partition. So having more consumers in a consumer-group than the number of brokers does not
            have any benefit. Since in our kafka cluster we have only one broker so out topic has only one partition
            and our consumer group have two consumers, now if you publish messages to the topic you will find only one
            consumer is reading all the messages and another one is just sitting idle (until you terminate the first
            consumer or restart that).
            3.) Suppose a topic has 3 partitions and our consumer group has 2 consumers.So consumer1 is assigned
            partition1 and partition2 and consumer2 is assigned partitions3.Now suppose you added one more consumer to
            the consumer group.In that case reassignment of partitions will take place among all consumers, and now it
            might be possible that consumer1 get partition1, consumer2 get partition2 and consumer3 get partition3. So
            which consumer will get which consumer we can't know.
            ****************************************************************************************************
         */

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
