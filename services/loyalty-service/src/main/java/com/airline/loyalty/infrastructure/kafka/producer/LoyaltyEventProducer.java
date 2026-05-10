package com.airline.loyalty.infrastructure.kafka.producer;

import com.airline.common.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class LoyaltyEventProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LoyaltyEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPointsReserved(Object payload, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, payload);
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("loyalty.points.reserved.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publishPointsDeducted(Object payload, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, payload);
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("loyalty.points.deducted.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
