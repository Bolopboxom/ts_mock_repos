package com.airline.notification.infrastructure.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class NotificationConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "booking.confirmed.v1", groupId = "notification-service-group")
    public void onBookingConfirmed(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String ticketNumber = payload.has("ticketNumber") ? payload.get("ticketNumber").asText() : "";
            System.out.println("Notification: send email for booking=" + bookingId + " ticket=" + ticketNumber);
        } catch (Exception e) {
            System.err.println("NotificationConsumer error: " + e.getMessage());
        }
    }
}
