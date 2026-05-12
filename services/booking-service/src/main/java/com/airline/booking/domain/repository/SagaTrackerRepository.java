package com.airline.booking.domain.repository;

import com.airline.booking.domain.model.SagaTracker;

import java.util.List;
import java.util.Optional;

public interface SagaTrackerRepository {
    void save(SagaTracker tracker);
    Optional<SagaTracker> findByTransactionId(String transactionId);
    Optional<SagaTracker> findByBookingId(String bookingId);
    List<SagaTracker> findAll();
    List<SagaTracker> findByStatus(SagaTracker.SagaStatus status);
}
