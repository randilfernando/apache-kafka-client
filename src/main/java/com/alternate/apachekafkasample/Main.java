package com.alternate.apachekafkasample;

import com.alternate.apachekafkasample.services.Consumer;
import com.alternate.apachekafkasample.services.Producer;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter bootstrap string: ");
        String bootstrapString = scanner.nextLine();

        Consumer consumer = new Consumer(bootstrapString);
        Producer producer = new Producer(bootstrapString);

        System.out.println("Select mode (1 - Producer 2 - Consumer)");
        System.out.print("Enter selection (1/2): ");

        String mode = scanner.nextLine();

        if ("1".equals(mode)) {
            producer.start();
        } else if ("2".equals(mode)) {
            consumer.start();
        } else {
            throw new UnsupportedOperationException("invalid choice");
        }
    }
}
