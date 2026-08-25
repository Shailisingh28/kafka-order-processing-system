package com.shaili.kafka_order_system.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class NotificationConsumer {
    private final Counter notificationsSentCounter;

    NotificationConsumer(MeterRegistry meterRegistry) {
        this.notificationsSentCounter = Counter.builder("notifications.sent").description("Total notifications sent")
                .register(meterRegistry);
    }

    @KafkaListener(topics = "orders", groupId = "notification-group")
    public void sendNotification(String message) {
        String[] parts = message.split("\\|", 2);
        String orderId = parts[0];
        String item = parts[1];

        if (item.equals("FAIL")) {
            return;
        }

        System.out.println("[Notification Service] Sending confirmation for orderId=" + orderId + ", item=" + item);
        notificationsSentCounter.increment();
    }
}
