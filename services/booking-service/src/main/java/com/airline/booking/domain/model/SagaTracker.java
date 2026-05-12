package com.airline.booking.domain.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class SagaTracker {
    
    private String transactionId; // same as correlationId
    private String bookingId;
    private SagaStatus status;
    private LocalDateTime startedAt;
    private LocalDateTime completedAt;
    private List<SagaStep> steps = new ArrayList<>();
    
    public enum SagaStatus {
        IN_PROGRESS,
        COMPLETED,
        COMPENSATING,
        FAILED
    }
    
    @Data
    @NoArgsConstructor
    public static class SagaStep {
        private String serviceName;
        private String eventType;
        private StepStatus status;
        private LocalDateTime timestamp;
        private String details;
        
        public enum StepStatus {
            SUCCESS,
            FAILED,
            COMPENSATED
        }
        
        public SagaStep(String serviceName, String eventType, StepStatus status, String details) {
            this.serviceName = serviceName;
            this.eventType = eventType;
            this.status = status;
            this.timestamp = LocalDateTime.now();
            this.details = details;
        }
    }
    
    public SagaTracker(String transactionId, String bookingId) {
        this.transactionId = transactionId;
        this.bookingId = bookingId;
        this.status = SagaStatus.IN_PROGRESS;
        this.startedAt = LocalDateTime.now();
        this.steps = new ArrayList<>();
    }
    
    public void addStep(String serviceName, String eventType, SagaStep.StepStatus stepStatus, String details) {
        SagaStep step = new SagaStep(serviceName, eventType, stepStatus, details);
        this.steps.add(step);
    }
    
    public void markCompleted() {
        this.status = SagaStatus.COMPLETED;
        this.completedAt = LocalDateTime.now();
    }
    
    public void markCompensating() {
        this.status = SagaStatus.COMPENSATING;
    }
    
    public void markFailed() {
        this.status = SagaStatus.FAILED;
        this.completedAt = LocalDateTime.now();
    }
    
    // Helper method to get progress percentage
    public double getProgressPercentage() {
        if (steps.isEmpty()) return 0.0;
        long successSteps = steps.stream()
            .filter(s -> s.getStatus() == SagaStep.StepStatus.SUCCESS)
            .count();
        return (double) successSteps / steps.size() * 100.0;
    }
    
    // Helper method to get current step
    public String getCurrentStep() {
        if (steps.isEmpty()) return "Not started";
        SagaStep lastStep = steps.get(steps.size() - 1);
        return lastStep.getServiceName() + ":" + lastStep.getEventType();
    }
}
