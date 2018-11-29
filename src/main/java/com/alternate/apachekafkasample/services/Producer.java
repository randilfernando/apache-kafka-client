package com.alternate.apachekafkasample.services;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class Producer {
    private final KafkaProducer<String, String> producer;

    public Producer(String bootstrapString) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapString);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(props);
    }

    public void start() {
        System.out.println("============================== Producer started ==============================");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter topic: ");
        String topic = scanner.nextLine();

        while (true) {
            System.out.println("==============================================================================");
            System.out.print("Enter key: ");
            String key = scanner.nextLine();
            System.out.print("Enter value: ");
            String value = scanner.nextLine();
            this.producer.send(new ProducerRecord<>(topic, key, value));
            this.producer.flush();
            System.out.println("================================ Record sent =================================");
        }
    }
}
