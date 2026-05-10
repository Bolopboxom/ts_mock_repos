package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentCompletedConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BookingRepository bookingRepository;
    private final BookingEventProducer producer;

    public PaymentCompletedConsumer(BookingRepository bookingRepository, BookingEventProducer producer) {
        this.bookingRepository = bookingRepository;
        this.producer = producer;
    }

    @KafkaListener(topics = "payment.completed.v1", groupId = "booking-service-group")
    public void onPaymentCompleted(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            // update booking to CONFIRMED
            var booking = bookingRepository.findById(bookingId);
            if (booking != null) {
                booking.setStatus(com.airline.booking.domain.model.Booking.Status.CONFIRMED);
                booking.setTicketNumber("TKT-" + bookingId.substring(Math.max(0, bookingId.length()-6)));
                bookingRepository.save(booking);
                // publish booking.confirmed.v1
                Object pay = objectMapper.createObjectNode().put("bookingId", bookingId).put("ticketNumber", booking.getTicketNumber());
                String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
                producer.publishBookingConfirmed(objectMapper.writeValueAsString(pay), correlationId);
            }
        } catch (Exception e) {
            System.err.println("PaymentCompletedConsumer error: " + e.getMessage());
        }
    }
}
