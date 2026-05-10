package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentFailedConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BookingEventProducer producer;
    private final BookingRepository bookingRepository;

    public PaymentFailedConsumer(BookingEventProducer producer, BookingRepository bookingRepository) {
        this.producer = producer;
        this.bookingRepository = bookingRepository;
    }

    @KafkaListener(topics = "payment.failed.v1", groupId = "booking-service-group")
    public void onPaymentFailed(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            System.out.println("Payment failed for booking=" + bookingId + " -> cancelling booking and publishing booking.cancelled.v1");

            // build cancellation payload and publish
            JsonNode cancelNode = objectMapper.createObjectNode()
                    .put("bookingId", bookingId)
                    .put("reason", payload.has("reason") ? payload.get("reason").asText() : "payment_failed");
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            String cancelJson = objectMapper.writeValueAsString(cancelNode);
            producer.publishBookingCancelled(cancelJson, correlationId);

            // update booking state to CANCELLED if exists
            var booking = bookingRepository.findById(bookingId);
            if (booking != null) {
                booking.setStatus(com.airline.booking.domain.model.Booking.Status.CANCELLED);
                bookingRepository.save(booking);
                System.out.println("Booking " + bookingId + " set to CANCELLED");
            }
        } catch (Exception e) {
            System.err.println("PaymentFailedConsumer error: " + e.getMessage());
        }
    }
}
