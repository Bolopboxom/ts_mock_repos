package com.airline.booking.application.usecase;

import com.airline.booking.application.dto.BookingDto;
import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class CreateBookingUseCase {

    private final BookingRepository bookingRepository;
    private final BookingEventProducer producer;

    public CreateBookingUseCase(BookingRepository bookingRepository, BookingEventProducer producer) {
        this.bookingRepository = bookingRepository;
        this.producer = producer;
    }

    public String execute(BookingDto dto, String correlationId) {
        String bookingId = "BKG-" + UUID.randomUUID().toString().substring(0,8);
        Booking booking = new Booking(bookingId, dto.customerId, dto.flightId);
        bookingRepository.save(booking);
        // publish event (payload as simple JSON string for now)
        String payload = String.format("{\"bookingId\":\"%s\",\"customerId\":\"%s\",\"flightId\":\"%s\",\"passengers\":%d,\"usePoints\":%d}",
                bookingId, dto.customerId, dto.flightId, dto.passengers, dto.usePoints);
        producer.publishBookingCreated(payload, correlationId);
        return bookingId;
    }
}
