package com.shaili.kafka_order_system.consumer;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    // private final Set<String> processedOrderIds = ConcurrentHashMap.newKeySet();
    private StringRedisTemplate redisTemplate;

    OrderConsumer(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consumeOrderEvent(String message) {
        String[] parts = message.split("\\|", 2);
        String orderId = parts[0];
        String item = parts[1];
        // yaha "processed-order:" ye prefix isliye add kiya hai taaki naming convention
        // ki tarah use kar sake.agar akbhi aage chal k bahut saare data store hue redis
        // mein toh iss naming convention se pta chal payega kiska kya kaam hai
        String redisKey = "processed-order:" + orderId;
        Boolean alreadyProcessed = redisTemplate.hasKey(redisKey);
        // Idempotency
        // if (processedOrderIds.contains(orderId)) {
        // System.out.println("Duplicate detected, skipping: orderId=" + orderId);
        // return;
        // }
        if (Boolean.TRUE.equals(alreadyProcessed)) {
            System.out.println("Duplicate detected (via Redis), skipping: orderId=" + orderId);
            return;
        }
        System.out.println("Processing new order: orderId=" + orderId + ", item=" + item);
        // Redis mein mark karo ki yeh orderId process ho gaya - 24 ghante ke liye rakho
        redisTemplate.opsForValue().set(redisKey, "true", 24, TimeUnit.HOURS);
    }

}
