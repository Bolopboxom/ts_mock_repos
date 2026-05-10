package com.airline.seat.application;

import com.airline.seat.infrastructure.kafka.producer.SeatEventProducer;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class SeatService {
    private final Map<String, String> reservations = new ConcurrentHashMap<>();
    private final SeatEventProducer producer;

    public SeatService(SeatEventProducer producer) {
        this.producer = producer;
    }

    public void reserveSeat(String bookingId, String flightId, String correlationId) {
        // simple in-memory reserve for demo
        String seatInfo = "{\"seat\":\"1A\"}";
        reservations.put(bookingId, seatInfo);
        System.out.println("Seat reserved for booking=" + bookingId + " corr=" + correlationId);
        // publish seat.reserved.v1
        producer.publishSeatReserved(bookingId, flightId, correlationId, seatInfo);
    }
}
