package com.airline.payment.infrastructure.kafka.producer;

import com.airline.common.EventEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class PaymentEventProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentCompleted(Object payload, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, payload);
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("payment.completed.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void publishPaymentFailed(Object payload, String correlationId) {
        try {
            EventEnvelope envelope = new EventEnvelope(correlationId, payload);
            String message = objectMapper.writeValueAsString(envelope);
            kafkaTemplate.send("payment.failed.v1", envelope.eventId, message);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
