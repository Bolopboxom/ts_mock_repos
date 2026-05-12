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
public class PaymentFailedConsumer {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BookingEventProducer producer;
    private final BookingRepository bookingRepository;
    private final SagaTrackingService sagaTrackingService;

    public PaymentFailedConsumer(BookingEventProducer producer, 
                                 BookingRepository bookingRepository,
                                 SagaTrackingService sagaTrackingService) {
        this.producer = producer;
        this.bookingRepository = bookingRepository;
        this.sagaTrackingService = sagaTrackingService;
    }

    @KafkaListener(topics = "payment.failed.v1", groupId = "booking-service-group")
    public void onPaymentFailed(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            String reason = payload.has("reason") ? payload.get("reason").asText() : "payment_failed";
            
            log.info("Payment failed for booking={} -> cancelling booking and publishing booking.cancelled.v1", bookingId);

            // Track failure and mark saga as compensating
            if (correlationId != null) {
                sagaTrackingService.recordFailure(correlationId, "payment-service", "payment.failed.v1",
                    "Payment failed: " + reason);
            }

            // build cancellation payload and publish
            JsonNode cancelNode = objectMapper.createObjectNode()
                    .put("bookingId", bookingId)
                    .put("reason", reason);
            String cancelJson = objectMapper.writeValueAsString(cancelNode);
            producer.publishBookingCancelled(cancelJson, correlationId);

            // update booking state to CANCELLED if exists
            var booking = bookingRepository.findById(bookingId);
            if (booking != null) {
                booking.setStatus(com.airline.booking.domain.model.Booking.Status.CANCELLED);
                bookingRepository.save(booking);
                
                // Track compensation and mark saga as failed
                if (correlationId != null) {
                    sagaTrackingService.recordCompensation(correlationId, "booking-service", "booking.cancelled.v1",
                        "Booking cancelled due to payment failure");
                    sagaTrackingService.failSaga(correlationId);
                }
                
                log.info("Booking {} set to CANCELLED", bookingId);
            }
        } catch (Exception e) {
            log.error("PaymentFailedConsumer error: {}", e.getMessage(), e);
        }
    }
}
