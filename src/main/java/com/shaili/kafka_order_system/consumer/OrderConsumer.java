package com.shaili.kafka_order_system.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consumeOrderEvent(String message) {
        System.out.println("Received from Kafka: " + message);
        // Yaha future mein real processing hogi - jaise inventory check, payment, etc.
    }

}
