package com.airline.booking.infrastructure.kafka.producer;

import com.airline.common.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BookingEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public BookingEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishBookingCreated(String payloadJson, String correlationId) {
        try {
            // for demo, payloadJson is already a JSON string; wrap it in envelope as generic Object
            EventEnvelope envelope = new EventEnvelope(correlationId, objectMapper.readTree(payloadJson));
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("booking.created.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publishBookingConfirmed(String payloadJson, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, objectMapper.readTree(payloadJson));
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("booking.confirmed.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publishBookingCancelled(String payloadJson, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, objectMapper.readTree(payloadJson));
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("booking.cancelled.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
