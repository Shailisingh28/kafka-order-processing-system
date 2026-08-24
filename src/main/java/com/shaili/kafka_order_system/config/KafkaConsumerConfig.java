package com.shaili.kafka_order_system.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {
    private KafkaTemplate<String, String> kafkaTemplate;

    KafkaConsumerConfig(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    public CommonErrorHandler errorHandler() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, exception) -> new org.apache.kafka.common.TopicPartition("orders-dlq", record.partition()));
        FixedBackOff backOff = new FixedBackOff(2000L, 3);
        return new org.springframework.kafka.listener.DefaultErrorHandler(recoverer, backOff);
    }
}
