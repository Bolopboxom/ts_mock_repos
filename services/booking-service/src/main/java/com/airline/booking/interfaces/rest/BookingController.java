package com.airline.booking.interfaces.rest;

import com.airline.booking.application.dto.BookingDto;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bookings")
public class BookingController {

    @PostMapping
    public ResponseEntity<String> createBooking(@RequestBody BookingDto dto) {
        // TODO: call use-case to create booking and publish booking.created.v1
        return ResponseEntity.accepted().body("booking pending");
    }
}
