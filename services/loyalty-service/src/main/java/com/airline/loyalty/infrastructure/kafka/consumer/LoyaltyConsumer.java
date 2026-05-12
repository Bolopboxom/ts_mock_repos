package com.airline.loyalty.infrastructure.kafka.consumer;

import com.airline.loyalty.infrastructure.kafka.producer.LoyaltyEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LoyaltyConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final LoyaltyEventProducer producer;

    public LoyaltyConsumer(LoyaltyEventProducer producer) {
        this.producer = producer;
    }

    @KafkaListener(topics = "seat.reserved.v1", groupId = "loyalty-service-group")
    public void onSeatReserved(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String customerId = payload.has("customerId") ? payload.get("customerId").asText() : "UNKNOWN";
            int points = payload.has("points") ? payload.get("points").asInt() : 0;
            // reserve points (demo)
            log.info("Loyalty reserve for booking={} customer={}", bookingId, customerId);
            // publish loyalty.points.reserved.v1
            Object pay = objectMapper.createObjectNode()
                    .put("bookingId", bookingId)
                    .put("customerId", customerId)
                    .put("points", points);
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            producer.publishPointsReserved(pay, correlationId);
        } catch (Exception e) {
            log.error("LoyaltyConsumer error: {}", e.getMessage(), e);
        }
    }
}
