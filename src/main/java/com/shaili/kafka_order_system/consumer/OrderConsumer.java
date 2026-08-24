package com.shaili.kafka_order_system.consumer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class OrderConsumer {

    private StringRedisTemplate redisTemplate;
    private final Counter ordersProcessedCounter;
    private final Counter ordersDuplicateCounter;
    private final Counter ordersFailedCounter;
    private final Timer processingTimer;

    public OrderConsumer(StringRedisTemplate redisTemplate, MeterRegistry meterRegistry) {
        this.redisTemplate = redisTemplate;
        this.ordersProcessedCounter = Counter.builder("orders.processed")
                .description("Total orders successfully processed")
                .register(meterRegistry);

        this.ordersDuplicateCounter = Counter.builder("orders.duplicate")
                .description("Total duplicate orders detected and skipped")
                .register(meterRegistry);

        this.ordersFailedCounter = Counter.builder("orders.failed")
                .description("Total orders that threw an exception during processing")
                .register(meterRegistry);

        this.processingTimer = Timer.builder("orders.processing.time")
                .description("Time taken to process an order")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consumeOrderEvent(String message) {
        Timer.Sample sample = Timer.start();

        try {
            String[] parts = message.split("\\|", 2);
            String orderId = parts[0];
            String item = parts[1];

            if (item.equals("FAIL")) {
                ordersFailedCounter.increment();
                throw new RuntimeException("Simulated processing failure for testing DLQ");
            }

            String redisKey = "processed-order:" + orderId;
            Boolean alreadyProcessed = redisTemplate.hasKey(redisKey);

            if (Boolean.TRUE.equals(alreadyProcessed)) {
                ordersDuplicateCounter.increment();
                System.out.println("Duplicate detected (via Redis), skipping: orderId=" + orderId);
                return;
            }

            System.out.println("Processing new order: orderId=" + orderId + ", item=" + item);
            redisTemplate.opsForValue().set(redisKey, "true", 24, TimeUnit.HOURS);
            ordersProcessedCounter.increment();

        } finally {
            sample.stop(processingTimer);
        }
    }
}