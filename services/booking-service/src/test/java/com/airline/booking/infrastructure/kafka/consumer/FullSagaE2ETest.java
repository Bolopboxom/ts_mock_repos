package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.BookingApplication;
import com.airline.booking.application.service.SagaTrackingService;
import com.airline.booking.application.usecase.CreateBookingUseCase;
import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.model.SagaTracker;
import com.airline.booking.domain.repository.BookingRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.concurrent.BlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = BookingApplication.class, properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"booking.created.v1", "seat.reserved.v1", "loyalty.points.reserved.v1", "payment.completed.v1", "booking.confirmed.v1", "loyalty.points.deducted.v1"})
@Import(SimulatorsTestConfig.class)
@ActiveProfiles("demo")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class FullSagaE2ETest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private BlockingQueue<String> deductedQueue;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
    
    @Autowired
    private SagaTrackingService sagaTrackingService;
    
    @Autowired
    private CreateBookingUseCase createBookingUseCase;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private void waitForListenerContainers(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allRunning = true;
            boolean allAssigned = true;
            for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
                if (!container.isRunning()) {
                    allRunning = false;
                    break;
                }
                if (container instanceof org.springframework.kafka.listener.KafkaMessageListenerContainer) {
                    org.springframework.kafka.listener.KafkaMessageListenerContainer<?,?> kafkaContainer = (org.springframework.kafka.listener.KafkaMessageListenerContainer<?,?>) container;
                    try {
                        java.util.Collection<org.apache.kafka.common.TopicPartition> assigned = kafkaContainer.getAssignedPartitions();
                        if (assigned == null || assigned.isEmpty()) {
                            allAssigned = false;
                            break;
                        }
                    } catch (UnsupportedOperationException ignored) {
                        // older versions may not support getAssignedPartitions
                    }
                }
            }
            if (allRunning && allAssigned) {
                Thread.sleep(500);
                return;
            }
            Thread.sleep(100);
        }
    }

    @Test
    @DirtiesContext
    void endToEnd_happyPath_allSteps() throws Exception {
        // CRITICAL: Wait for Kafka consumers FIRST, before publishing any events
        // This prevents lost events when test runs in suite after other tests
        System.out.println("[test] Waiting for Kafka listeners to be ready...");
        waitForListenerContainers(15000);
        
        int containerCount = 0;
        Object containersObj = kafkaListenerEndpointRegistry.getListenerContainers();
        if (containersObj == null) {
            containerCount = 0;
        } else if (containersObj.getClass().isArray()) {
            containerCount = java.lang.reflect.Array.getLength(containersObj);
        } else if (containersObj instanceof java.util.Collection) {
            containerCount = ((java.util.Collection<?>) containersObj).size();
        } else {
            containerCount = 1; // fallback
        }
        System.out.println("[test] listener containers count=" + containerCount);
        Thread.sleep(2000); // Extra buffer for full initialization
        
        // NOW create booking - listeners are ready
        com.airline.booking.application.dto.BookingDto dto = new com.airline.booking.application.dto.BookingDto();
        dto.customerId = "CUST-1";
        dto.flightId = "FL-1";
        dto.seatClass = "ECONOMY";
        dto.passengers = 1;
        dto.usePoints = 100;
        
        String correlationId = "corr-e2e-1";
        String bookingId = createBookingUseCase.execute(dto, correlationId);
        
        System.out.println("[test] Booking created with ID: " + bookingId + ", correlationId: " + correlationId);

        // CreateBookingUseCase already published booking.created.v1 event
        System.out.println("[test] Waiting for saga to complete...");

        // wait for final deducted event (signals saga completion)
        String deducted = deductedQueue.poll(25, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(deducted != null && deducted.contains(bookingId), "Expected loyalty.points.deducted.v1 containing bookingId");

        // booking should have been confirmed as part of the saga
        long deadline = System.currentTimeMillis() + 2000;
        boolean confirmed = false;
        while (System.currentTimeMillis() < deadline) {
            Booking saved = bookingRepository.findById(bookingId);
            if (saved != null && saved.getStatus() == Booking.Status.CONFIRMED) { confirmed = true; break; }
            Thread.sleep(50);
        }
        assertTrue(confirmed, "Booking should be CONFIRMED by end of saga");
        
        // Verify saga tracking
        var sagaTrackerOpt = sagaTrackingService.getSagaByTransactionId("corr-e2e-1");
        assertTrue(sagaTrackerOpt.isPresent(), "Saga tracker should exist for correlationId");
        
        var sagaTracker = sagaTrackerOpt.get();
        
        // Print saga details
        printSagaDetails(sagaTracker);
        
        // Assertions
        assertEquals(SagaTracker.SagaStatus.COMPLETED, sagaTracker.getStatus(),
            "Saga should be marked as COMPLETED");
        assertEquals(bookingId, sagaTracker.getBookingId(), "Saga should track correct booking ID");
        assertTrue(sagaTracker.getSteps().size() >= 5, 
            "Saga should have at least 5 steps (booking.created, seat.reserved, loyalty.reserved, payment.completed, booking.confirmed)");
    }

    /**
     * Helper method to print saga tracking details in a readable format
     */
    private void printSagaDetails(SagaTracker saga) {
        System.out.println("\n╔════════════════════════════════════════════════════════════════╗");
        System.out.println("║              SAGA TRACKING DETAILS                             ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.println(String.format("  Transaction ID : %s", saga.getTransactionId()));
        System.out.println(String.format("  Booking ID     : %s", saga.getBookingId()));
        System.out.println(String.format("  Status         : %s", saga.getStatus()));
        System.out.println(String.format("  Started At     : %s", saga.getStartedAt()));
        System.out.println(String.format("  Completed At   : %s", saga.getCompletedAt() != null ? saga.getCompletedAt() : "In Progress"));
        System.out.println(String.format("  Progress       : %.1f%%", saga.getProgressPercentage()));
        System.out.println(String.format("  Current Step   : %s", saga.getCurrentStep()));
        System.out.println(String.format("  Total Steps    : %d", saga.getSteps().size()));
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        System.out.println("║  SAGA FLOW:                                                    ║");
        System.out.println("╠════════════════════════════════════════════════════════════════╣");
        
        for (int i = 0; i < saga.getSteps().size(); i++) {
            SagaTracker.SagaStep step = saga.getSteps().get(i);
            String statusIcon = switch (step.getStatus()) {
                case SUCCESS -> "✓";
                case FAILED -> "✗";
                case COMPENSATED -> "↩";
            };
            System.out.println(String.format("  %d. [%s %s] %s", 
                i + 1, statusIcon, step.getStatus(), step.getServiceName()));
            System.out.println(String.format("     Event: %s", step.getEventType()));
            System.out.println(String.format("     Time : %s", step.getTimestamp()));
            System.out.println(String.format("     Info : %s", step.getDetails()));
            if (i < saga.getSteps().size() - 1) {
                System.out.println("     ↓");
            }
        }
        System.out.println("╚════════════════════════════════════════════════════════════════╝\n");
    }

    // Simulators moved to top-level SimulatorsTestConfig
}
