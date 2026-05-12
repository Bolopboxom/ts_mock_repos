package com.airline.seat.infrastructure.kafka.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ReleaseSeatConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "seat.released.v1", groupId = "seat-service-group")
    public void onSeatReleased(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            log.info("Seat release for bookingId={}", bookingId);
            // release seat in in-memory store (not implemented)
        } catch (Exception e) {
            log.error("ReleaseSeatConsumer error: {}", e.getMessage(), e);
        }
    }
}
