package com.airline.loyalty.infrastructure.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class LoyaltyConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "seat.reserved.v1", groupId = "loyalty-service-group")
    public void onSeatReserved(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String customerId = payload.has("customerId") ? payload.get("customerId").asText() : "UNKNOWN";
            int points = payload.has("points") ? payload.get("points").asInt() : 0;
            // reserve points (demo)
            System.out.println("Loyalty reserve for booking=" + bookingId + " customer=" + customerId);
            // publish loyalty.points.reserved.v1 (skipped code for brevity)
        } catch (Exception e) {
            System.err.println("LoyaltyConsumer error: " + e.getMessage());
        }
    }
}
