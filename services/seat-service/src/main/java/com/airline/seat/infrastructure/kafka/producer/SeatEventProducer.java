package com.airline.seat.infrastructure.kafka.producer;

import com.airline.common.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class SeatEventProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public SeatEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishSeatReserved(String bookingId, String flightId, String correlationId, Object seatInfo) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, seatInfo);
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("seat.reserved.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
