package com.airline.booking.infrastructure.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class BookingEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public BookingEventProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishBookingCreated(String payloadJson, String correlationId) {
        // TODO: wrap payload into envelope with correlationId and publish to topic
        kafkaTemplate.send("booking.created.v1", payloadJson);
    }
}
