package com.airline.booking.interfaces.rest;

import com.airline.booking.application.service.SagaTrackingService;
import com.airline.booking.domain.model.SagaTracker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/saga-tracking")
@Slf4j
public class SagaTrackingController {
    
    private final SagaTrackingService trackingService;
    
    public SagaTrackingController(SagaTrackingService trackingService) {
        this.trackingService = trackingService;
    }
    
    /**
     * GET /saga-tracking/transaction/{transactionId}
     * Get saga status by transaction ID (correlationId)
     */
    @GetMapping("/transaction/{transactionId}")
    public ResponseEntity<SagaTrackerResponse> getByTransactionId(@PathVariable String transactionId) {
        Optional<SagaTracker> tracker = trackingService.getSagaByTransactionId(transactionId);
        return tracker
            .map(t -> ResponseEntity.ok(toResponse(t)))
            .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * GET /saga-tracking/booking/{bookingId}
     * Get saga status by booking ID
     */
    @GetMapping("/booking/{bookingId}")
    public ResponseEntity<SagaTrackerResponse> getByBookingId(@PathVariable String bookingId) {
        Optional<SagaTracker> tracker = trackingService.getSagaByBookingId(bookingId);
        return tracker
            .map(t -> ResponseEntity.ok(toResponse(t)))
            .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * GET /saga-tracking/all
     * Get all sagas
     */
    @GetMapping("/all")
    public ResponseEntity<List<SagaTrackerResponse>> getAll() {
        List<SagaTracker> trackers = trackingService.getAllSagas();
        List<SagaTrackerResponse> responses = trackers.stream()
            .map(this::toResponse)
            .toList();
        return ResponseEntity.ok(responses);
    }
    
    /**
     * GET /saga-tracking/in-progress
     * Get all in-progress sagas
     */
    @GetMapping("/in-progress")
    public ResponseEntity<List<SagaTrackerResponse>> getInProgress() {
        List<SagaTracker> trackers = trackingService.getInProgressSagas();
        List<SagaTrackerResponse> responses = trackers.stream()
            .map(this::toResponse)
            .toList();
        return ResponseEntity.ok(responses);
    }
    
    private SagaTrackerResponse toResponse(SagaTracker tracker) {
        return new SagaTrackerResponse(
            tracker.getTransactionId(),
            tracker.getBookingId(),
            tracker.getStatus().name(),
            tracker.getStartedAt(),
            tracker.getCompletedAt(),
            tracker.getProgressPercentage(),
            tracker.getCurrentStep(),
            tracker.getSteps().stream()
                .map(step -> new StepResponse(
                    step.getServiceName(),
                    step.getEventType(),
                    step.getStatus().name(),
                    step.getTimestamp(),
                    step.getDetails()
                ))
                .toList()
        );
    }
    
    public record SagaTrackerResponse(
        String transactionId,
        String bookingId,
        String status,
        java.time.LocalDateTime startedAt,
        java.time.LocalDateTime completedAt,
        double progressPercentage,
        String currentStep,
        List<StepResponse> steps
    ) {}
    
    public record StepResponse(
        String serviceName,
        String eventType,
        String status,
        java.time.LocalDateTime timestamp,
        String details
    ) {}
}
