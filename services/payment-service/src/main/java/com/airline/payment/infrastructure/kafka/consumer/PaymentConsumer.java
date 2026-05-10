package com.airline.payment.infrastructure.kafka.consumer;

import com.airline.payment.infrastructure.kafka.producer.PaymentEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PaymentEventProducer producer;

    public PaymentConsumer(PaymentEventProducer producer) {
        this.producer = producer;
    }

    @KafkaListener(topics = "loyalty.points.reserved.v1", groupId = "payment-service-group")
    public void onPointsReserved(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            // mock payment success
            System.out.println("Payment processed for booking=" + bookingId);
            Object pay = objectMapper.createObjectNode()
                    .put("bookingId", bookingId)
                    .put("transactionId", "txn-" + bookingId);
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            producer.publishPaymentCompleted(pay, correlationId);
        } catch (Exception e) {
            System.err.println("PaymentConsumer error: " + e.getMessage());
        }
    }
}
