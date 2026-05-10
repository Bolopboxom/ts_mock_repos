package com.airline.booking.interfaces.rest;

import com.airline.booking.application.dto.BookingDto;
import com.airline.booking.application.usecase.CreateBookingUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/bookings")
public class BookingController {

    private final CreateBookingUseCase createBookingUseCase;

    public BookingController(CreateBookingUseCase createBookingUseCase) {
        this.createBookingUseCase = createBookingUseCase;
    }

    @PostMapping
    public ResponseEntity<String> createBooking(@RequestBody BookingDto dto,
                                                @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = "corr-" + UUID.randomUUID().toString().substring(0,8);
        }
        String bookingId = createBookingUseCase.execute(dto, correlationId);
        return ResponseEntity.accepted().body(bookingId);
    }
}
