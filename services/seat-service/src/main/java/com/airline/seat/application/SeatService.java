package com.airline.seat.application;

import com.airline.seat.infrastructure.kafka.producer.SeatEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
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
        log.info("Seat reserved for booking={} corr={}", bookingId, correlationId);
        // publish seat.reserved.v1
        producer.publishSeatReserved(bookingId, flightId, correlationId, seatInfo);
    }
}
