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
        System.out.println("[PaymentCompletedConsumer] Received message: " + message);
        try {
            JsonNode node = objectMapper.readTree(message);
            JsonNode payload = node.get("payload");
            String bookingId = payload.get("bookingId").asText();
            String correlationId = node.has("correlationId") ? node.get("correlationId").asText() : null;
            
            System.out.println("[PaymentCompletedConsumer] Processing bookingId=" + bookingId + ", correlationId=" + correlationId);
            
            // Track saga step
            if (correlationId != null) {
                try {
                    sagaTrackingService.recordStep(correlationId, "payment-service", "payment.completed.v1",
                        "Payment successful for booking: " + bookingId);
                    System.out.println("[PaymentCompletedConsumer] Saga step recorded");
                } catch (Exception e) {
                    System.err.println("[PaymentCompletedConsumer] SAGA TRACKING ERROR: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            
            // update booking to CONFIRMED
            var booking = bookingRepository.findById(bookingId);
            System.out.println("[PaymentCompletedConsumer] Fetched booking: " + (booking != null ? "FOUND" : "NOT FOUND"));
            if (booking != null) {
                System.out.println("[PaymentCompletedConsumer] Current booking status: " + booking.getStatus());
                booking.setStatus(com.airline.booking.domain.model.Booking.Status.CONFIRMED);
                booking.setTicketNumber("TKT-" + bookingId.substring(Math.max(0, bookingId.length()-6)));
                bookingRepository.save(booking);
                System.out.println("[PaymentCompletedConsumer] Booking confirmed and saved");
                
                // publish booking.confirmed.v1
                Object pay = objectMapper.createObjectNode().put("bookingId", bookingId).put("ticketNumber", booking.getTicketNumber());
                producer.publishBookingConfirmed(objectMapper.writeValueAsString(pay), correlationId);
                System.out.println("[PaymentCompletedConsumer] Published booking.confirmed.v1");
                
                // Track booking confirmed step and mark saga as completed
                if (correlationId != null) {
                    try {
                        sagaTrackingService.recordStep(correlationId, "booking-service", "booking.confirmed.v1",
                            "Booking confirmed with ticket: " + booking.getTicketNumber());
                        sagaTrackingService.completeSaga(correlationId);
                        System.out.println("[PaymentCompletedConsumer] Saga completed");
                    } catch (Exception e) {
                        System.err.println("[PaymentCompletedConsumer] SAGA COMPLETION ERROR: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
                
                log.info("Processed payment.completed.v1 for bookingId={}, published booking.confirmed.v1", bookingId);
            } else {
                System.err.println("[PaymentCompletedConsumer] ERROR: Booking not found for bookingId=" + bookingId);
            }
        } catch (Exception e) {
            log.error("PaymentCompletedConsumer error: {}", e.getMessage(), e);
        }
    }
}
