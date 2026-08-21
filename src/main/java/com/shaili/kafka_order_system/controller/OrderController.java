package com.shaili.kafka_order_system.controller;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.shaili.kafka_order_system.producer.OrderProducer;

@RestController
public class OrderController {
    OrderProducer orderProducer;

    OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/orders")
    public String placeOrder(@RequestParam String item) {
        String message = "Order Placed: " + item;
        orderProducer.sendOrderEvent(message);
        return "Order accepted: " + item;
    }
}
