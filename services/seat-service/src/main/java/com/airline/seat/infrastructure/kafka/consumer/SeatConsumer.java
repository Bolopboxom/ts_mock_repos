package com.airline.seat.infrastructure.kafka.consumer;

import com.airline.common.EventEnvelope;
import com.airline.seat.application.SeatService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SeatConsumer {

    private final SeatService seatService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SeatConsumer(SeatService seatService) {
        this.seatService = seatService;
    }

    @KafkaListener(topics = "booking.created.v1", groupId = "seat-service-group")
    public void onBookingCreated(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            // assume envelope
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String flightId = payload.get("flightId").asText();
            // perform reservation (in-memory)
            seatService.reserveSeat(bookingId, flightId, node.get("correlationId").asText());
        } catch (Exception e) {
            // simple log
            System.err.println("SeatConsumer failed to process message: " + e.getMessage());
        }
    }
}
