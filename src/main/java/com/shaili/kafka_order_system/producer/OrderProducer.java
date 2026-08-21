package com.shaili.kafka_order_system.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    private static final String TOPIC = "orders";
    private KafkaTemplate<String, String> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrderEvent(String orderMessage) {
        kafkaTemplate.send(TOPIC, orderMessage);
        System.out.println("Sent to Kafka: " + orderMessage);
    }
}
