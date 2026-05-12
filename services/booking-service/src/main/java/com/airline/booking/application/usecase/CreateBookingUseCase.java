package com.airline.booking.application.usecase;

import com.airline.booking.application.dto.BookingDto;
import com.airline.booking.application.service.SagaTrackingService;
import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class CreateBookingUseCase {

    private final BookingRepository bookingRepository;
    private final BookingEventProducer producer;
    private final SagaTrackingService sagaTrackingService;

    public CreateBookingUseCase(BookingRepository bookingRepository, 
                                BookingEventProducer producer,
                                SagaTrackingService sagaTrackingService) {
        this.bookingRepository = bookingRepository;
        this.producer = producer;
        this.sagaTrackingService = sagaTrackingService;
    }

    public String execute(BookingDto dto, String correlationId) {
        String bookingId = "BKG-" + UUID.randomUUID().toString().substring(0,8);
        Booking booking = new Booking(bookingId, dto.customerId, dto.flightId);
        bookingRepository.save(booking);
        
        // Initialize saga tracking
        try {
            sagaTrackingService.initializeSaga(correlationId, bookingId);
            sagaTrackingService.recordStep(correlationId, "booking-service", "booking.created.v1", 
                "Booking created: " + bookingId);
            System.out.println("[CreateBookingUseCase] Saga tracking initialized successfully");
        } catch (Exception e) {
            System.err.println("[CreateBookingUseCase] SAGA TRACKING ERROR: " + e.getMessage());
            e.printStackTrace();
        }
        
        // publish event (payload as simple JSON string for now)
        String payload = String.format("{\"bookingId\":\"%s\",\"customerId\":\"%s\",\"flightId\":\"%s\",\"passengers\":%d,\"usePoints\":%d}",
                bookingId, dto.customerId, dto.flightId, dto.passengers, dto.usePoints);
        producer.publishBookingCreated(payload, correlationId);
        System.out.println("[CreateBookingUseCase] Published booking.created.v1 for bookingId=" + bookingId);
        return bookingId;
    }
}
