package com.airline.booking.infrastructure.persistence;

import com.airline.booking.domain.model.SagaTracker;
import com.airline.booking.domain.repository.SagaTrackerRepository;
import org.springframework.stereotype.Repository;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Repository
public class InMemorySagaTrackerRepository implements SagaTrackerRepository {
    
    private final Map<String, SagaTracker> trackerByTransactionId = new ConcurrentHashMap<>();
    private final Map<String, String> bookingIdToTransactionId = new ConcurrentHashMap<>();
    
    @Override
    public void save(SagaTracker tracker) {
        trackerByTransactionId.put(tracker.getTransactionId(), tracker);
        if (tracker.getBookingId() != null) {
            bookingIdToTransactionId.put(tracker.getBookingId(), tracker.getTransactionId());
        }
    }
    
    @Override
    public Optional<SagaTracker> findByTransactionId(String transactionId) {
        return Optional.ofNullable(trackerByTransactionId.get(transactionId));
    }
    
    @Override
    public Optional<SagaTracker> findByBookingId(String bookingId) {
        String transactionId = bookingIdToTransactionId.get(bookingId);
        if (transactionId == null) {
            return Optional.empty();
        }
        return findByTransactionId(transactionId);
    }
    
    @Override
    public List<SagaTracker> findAll() {
        return new ArrayList<>(trackerByTransactionId.values());
    }
    
    @Override
    public List<SagaTracker> findByStatus(SagaTracker.SagaStatus status) {
        return trackerByTransactionId.values().stream()
            .filter(t -> t.getStatus() == status)
            .collect(Collectors.toList());
    }
}
