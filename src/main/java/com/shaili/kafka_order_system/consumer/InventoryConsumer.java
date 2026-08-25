package com.shaili.kafka_order_system.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

@Service
public class InventoryConsumer {
    private final Counter inventoryChecksCounter;

    public InventoryConsumer(MeterRegistry meterRegistry) {
        this.inventoryChecksCounter = Counter.builder("inventory.checks")
                .description("Total inventory checks performed").register(meterRegistry);
    }

    @KafkaListener(topics = "orders", groupId = "inventory-group")
    public void checkInventory(String message) {
        String[] parts = message.split("\\|", 2);
        String orderId = parts[0];
        String item = parts[1];
        if (item.equals("FAIL")) {
            return;
        }
        System.out.println("[Inventory Service] Checking stock for orderId=" + orderId + ", item=" + item);
        inventoryChecksCounter.increment();

    }
}
