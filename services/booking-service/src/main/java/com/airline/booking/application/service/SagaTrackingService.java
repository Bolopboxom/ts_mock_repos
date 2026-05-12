package com.airline.booking.application.service;

import com.airline.booking.domain.model.SagaTracker;
import com.airline.booking.domain.repository.SagaTrackerRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
@Slf4j
public class SagaTrackingService {
    
    private final SagaTrackerRepository repository;
    
    public SagaTrackingService(SagaTrackerRepository repository) {
        this.repository = repository;
    }
    
    /**
     * Initialize saga tracking when booking is created
     */
    public SagaTracker initializeSaga(String transactionId, String bookingId) {
        SagaTracker tracker = new SagaTracker(transactionId, bookingId);
        repository.save(tracker);
        log.info("Initialized saga tracking: transactionId={}, bookingId={}", transactionId, bookingId);
        return tracker;
    }
    
    /**
     * Record a successful step in the saga
     */
    public void recordStep(String transactionId, String serviceName, String eventType, String details) {
        Optional<SagaTracker> optTracker = repository.findByTransactionId(transactionId);
        if (optTracker.isPresent()) {
            SagaTracker tracker = optTracker.get();
            tracker.addStep(serviceName, eventType, SagaTracker.SagaStep.StepStatus.SUCCESS, details);
            repository.save(tracker);
            log.info("Recorded saga step: transactionId={}, service={}, event={}, progress={}%", 
                transactionId, serviceName, eventType, String.format("%.1f", tracker.getProgressPercentage()));
        } else {
            log.warn("Saga tracker not found for transactionId={}", transactionId);
        }
    }
    
    /**
     * Record a failed step in the saga
     */
    public void recordFailure(String transactionId, String serviceName, String eventType, String details) {
        Optional<SagaTracker> optTracker = repository.findByTransactionId(transactionId);
        if (optTracker.isPresent()) {
            SagaTracker tracker = optTracker.get();
            tracker.addStep(serviceName, eventType, SagaTracker.SagaStep.StepStatus.FAILED, details);
            tracker.markCompensating();
            repository.save(tracker);
            log.warn("Recorded saga failure: transactionId={}, service={}, event={}", 
                transactionId, serviceName, eventType);
        } else {
            log.warn("Saga tracker not found for transactionId={}", transactionId);
        }
    }
    
    /**
     * Record a compensation step
     */
    public void recordCompensation(String transactionId, String serviceName, String eventType, String details) {
        Optional<SagaTracker> optTracker = repository.findByTransactionId(transactionId);
        if (optTracker.isPresent()) {
            SagaTracker tracker = optTracker.get();
            tracker.addStep(serviceName, eventType, SagaTracker.SagaStep.StepStatus.COMPENSATED, details);
            repository.save(tracker);
            log.info("Recorded saga compensation: transactionId={}, service={}, event={}", 
                transactionId, serviceName, eventType);
        } else {
            log.warn("Saga tracker not found for transactionId={}", transactionId);
        }
    }
    
    /**
     * Mark saga as completed
     */
    public void completeSaga(String transactionId) {
        Optional<SagaTracker> optTracker = repository.findByTransactionId(transactionId);
        if (optTracker.isPresent()) {
            SagaTracker tracker = optTracker.get();
            tracker.markCompleted();
            repository.save(tracker);
            log.info("Saga completed: transactionId={}, totalSteps={}", transactionId, tracker.getSteps().size());
        } else {
            log.warn("Saga tracker not found for transactionId={}", transactionId);
        }
    }
    
    /**
     * Mark saga as failed
     */
    public void failSaga(String transactionId) {
        Optional<SagaTracker> optTracker = repository.findByTransactionId(transactionId);
        if (optTracker.isPresent()) {
            SagaTracker tracker = optTracker.get();
            tracker.markFailed();
            repository.save(tracker);
            log.info("Saga failed: transactionId={}", transactionId);
        } else {
            log.warn("Saga tracker not found for transactionId={}", transactionId);
        }
    }
    
    /**
     * Get saga status by transaction ID
     */
    public Optional<SagaTracker> getSagaByTransactionId(String transactionId) {
        return repository.findByTransactionId(transactionId);
    }
    
    /**
     * Get saga status by booking ID
     */
    public Optional<SagaTracker> getSagaByBookingId(String bookingId) {
        return repository.findByBookingId(bookingId);
    }
    
    /**
     * Get all in-progress sagas
     */
    public List<SagaTracker> getInProgressSagas() {
        return repository.findByStatus(SagaTracker.SagaStatus.IN_PROGRESS);
    }
    
    /**
     * Get all sagas
     */
    public List<SagaTracker> getAllSagas() {
        return repository.findAll();
    }
}
