package com.airline.notification.infrastructure.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class NotificationConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "booking.confirmed.v1", groupId = "notification-service-group")
    public void onBookingConfirmed(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String ticketNumber = payload.has("ticketNumber") ? payload.get("ticketNumber").asText() : "";
            log.info("Notification: send email for booking={} ticket={}", bookingId, ticketNumber);
        } catch (Exception e) {
            log.error("NotificationConsumer error: {}", e.getMessage(), e);
        }
    }
}
