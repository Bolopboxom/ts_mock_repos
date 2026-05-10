package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BookingEventProducer producer;

    public PaymentFailedConsumer(BookingEventProducer producer) {
        this.producer = producer;
    }

    @KafkaListener(topics = "payment.failed.v1", groupId = "booking-service-group")
    public void onPaymentFailed(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            System.out.println("Payment failed for booking=" + bookingId + " -> cancelling booking and publishing booking.cancelled.v1");
            Object pay = objectMapper.createObjectNode().put("bookingId", bookingId).put("reason", payload.has("reason")?payload.get("reason").asText():"");
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            producer.publishBookingCancelled(pay, correlationId);
            // update booking state to CANCELLED if exists
        } catch (Exception e) {
            System.err.println("PaymentFailedConsumer error: " + e.getMessage());
        }
    }
}
