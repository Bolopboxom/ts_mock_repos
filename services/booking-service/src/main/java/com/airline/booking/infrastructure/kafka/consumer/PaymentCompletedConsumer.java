package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.application.service.SagaTrackingService;
import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class PaymentCompletedConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BookingRepository bookingRepository;
    private final BookingEventProducer producer;
    private final SagaTrackingService sagaTrackingService;

    public PaymentCompletedConsumer(BookingRepository bookingRepository, 
                                    BookingEventProducer producer,
                                    SagaTrackingService sagaTrackingService) {
        this.bookingRepository = bookingRepository;
        this.producer = producer;
        this.sagaTrackingService = sagaTrackingService;
    }

    @KafkaListener(topics = "payment.completed.v1", groupId = "booking-service-group")
    public void onPaymentCompleted(String message) {
        log.info("Received payment.completed.v1: {}", message);
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            
            log.info("Processing payment completion for bookingId={}, correlationId={}", bookingId, correlationId);

            // Track saga step
            if (correlationId != null) {
                try {
                    sagaTrackingService.recordStep(correlationId, "payment-service", "payment.completed.v1",
                        "Payment successful for booking: " + bookingId);
                    log.debug("Saga step recorded for correlationId={}", correlationId);
                } catch (Exception e) {
                    log.error("SAGA TRACKING ERROR for correlationId={}: {}", correlationId, e.getMessage(), e);
                }
            }
            
            // update booking to CONFIRMED
            var booking = bookingRepository.findById(bookingId);
            log.debug("Fetched booking: {}", (booking != null ? "FOUND" : "NOT FOUND"));
            if (booking != null) {
                log.debug("Current booking status: {}", booking.getStatus());
                booking.setStatus(com.airline.booking.domain.model.Booking.Status.CONFIRMED);
                booking.setTicketNumber("TKT-" + bookingId.substring(Math.max(0, bookingId.length()-6)));
                bookingRepository.save(booking);
                log.info("Booking confirmed and saved: bookingId={}, ticketNumber={}", bookingId, booking.getTicketNumber());

                // publish booking.confirmed.v1
                Object pay = objectMapper.createObjectNode().put("bookingId", bookingId).put("ticketNumber", booking.getTicketNumber());
                producer.publishBookingConfirmed(objectMapper.writeValueAsString(pay), correlationId);
                log.info("Published booking.confirmed.v1 for bookingId={}", bookingId);

                // Track booking confirmed step and mark saga as completed
                if (correlationId != null) {
                    try {
                        sagaTrackingService.recordStep(correlationId, "booking-service", "booking.confirmed.v1",
                            "Booking confirmed with ticket: " + booking.getTicketNumber());
                        sagaTrackingService.completeSaga(correlationId);
                        log.info("Saga completed for correlationId={}", correlationId);
                    } catch (Exception e) {
                        log.error("SAGA COMPLETION ERROR for correlationId={}: {}", correlationId, e.getMessage(), e);
                    }
                }
                
                log.info("Processed payment.completed.v1 for bookingId={}, published booking.confirmed.v1", bookingId);
            } else {
                log.error("ERROR: Booking not found for bookingId={}", bookingId);
            }
        } catch (Exception e) {
            log.error("PaymentCompletedConsumer error: {}", e.getMessage(), e);
        }
    }
}
