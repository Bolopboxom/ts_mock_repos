package com.airline.booking.infrastructure.persistence;

import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class OracleBookingRepositoryImpl implements BookingRepository {
    private final Map<String, Booking> store = new ConcurrentHashMap<>();

    @Override
    public void save(Booking booking) {
        store.put(booking.getBookingId(), booking);
    }

    @Override
    public Booking findById(String bookingId) {
        return store.get(bookingId);
    }
}
