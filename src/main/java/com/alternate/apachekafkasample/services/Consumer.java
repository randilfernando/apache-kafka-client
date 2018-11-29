package com.alternate.apachekafkasample.services;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;

public class Consumer {
    private final KafkaConsumer<String, String> consumer;

    public Consumer(String bootstrapString) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapString);
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void start() {
        System.out.println("============================== Consumer started ==============================");

        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter topic name: ");
        String input = scanner.nextLine();
        consumer.subscribe(Collections.singleton(input));

        System.out.println("============================== Start listening ===============================");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("==============================================================================");
                System.out.printf("Key: %s", record.key());
                System.out.printf("Value: %s\n", record.value());
                System.out.println("==============================================================================");
            }
        }
    }
}
