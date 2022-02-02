package com.tp.kafka_basics.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "192.168.56.13:9092";

        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //Create a producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first-topic", "Message from remote server!");

        //Send data - Asynchronous operation
        producer.send(record);
        //If you run the code so far only then you will see no new message is published in the kafka-console-consumer.
        //It is because the above code will only store the message in the buffer and will not actually publish the
        //message into Kafka. So flushing of the producer is mandatory. We can do it in following two ways.

        //Flush data
        producer.flush();

        //Flush and Close
        producer.close();
    }
}
