package com.tp.kafka_basics.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {
        String bootstrapServers = "192.168.56.13:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        //Create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i=0; i<10; i++) {
            String topic = "first-topic";
            String message = "Hello World "+i+"!";
            String key;
            int finalI = i;
            if (IntStream.of(1,3,5,7,9).anyMatch(x -> x == finalI)){
                key = "key1";
            } else {
                key = "key2";
            }

            //Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);

            //Send data - Asynchronous operation
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //The record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition : " + recordMetadata.partition() + "\n" +
                                "Offset : " + recordMetadata.offset() + "\n" +
                                "Timestamp : " + recordMetadata.timestamp()+"\n"+
                                "Record message : "+ record.value());
                    } else {
                        //Got error in sending record
                        logger.error("Error while publishing record :", e);
                    }
                }
            });
            /*
                If your kafka cluster contains several brokers then in the above callback response you will find
                that all record messages which ends with 1,3,5,7 and 9 are published in same partition as key for
                all such messages is "key1" and all record messages which ends with 2,4,6,8 are published in same
                partition as key for all such messages is "key2".
                However, if your kafka cluster contains only one broker then all messages will be published in same
                partition as such clusters don't have any other partition available.
             */
        }
        //Flush data
        producer.flush();

        //Flush and Close
        producer.close();
    }
}
