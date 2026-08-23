package com.shaili.kafka_order_system.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();

    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consumeOrderEvent(String message) {
        String[] parts = message.split("\\|", 2);
        String orderId = parts[0];
        String item = parts[1];
        // Idempotency
        if (processedOrderIds.contains(orderId)) {
            System.out.println("Duplicate detected, skipping: orderId=" + orderId);
            return;
        }
        System.out.println("Processing new order: orderId=" + orderId + ", item=" + item);
        processedOrderIds.add(orderId);
    }

}
