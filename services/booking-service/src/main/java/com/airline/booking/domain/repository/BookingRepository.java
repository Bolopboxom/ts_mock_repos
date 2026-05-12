package com.airline.booking.domain.repository;

import com.airline.booking.domain.model.Booking;

public interface BookingRepository {
    void save(Booking booking);
    Booking findById(String bookingId);
}
