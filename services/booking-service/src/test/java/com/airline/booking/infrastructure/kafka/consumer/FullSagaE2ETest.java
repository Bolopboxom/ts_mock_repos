package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.BookingApplication;
import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.util.concurrent.BlockingQueue;

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
                Thread.sleep(150);
                return;
            }
            Thread.sleep(100);
        }
    }

    @Test
    void endToEnd_happyPath_allSteps() throws Exception {
        String bookingId = "BKG-E2E-1";
        Booking booking = new Booking(bookingId, "CUST-1", "FL-1");
        bookingRepository.save(booking);

        // wait for Kafka listener containers (simulators + consumers) to be running to avoid lost deliveries
        waitForListenerContainers(5000);

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

        // publish booking.created.v1 (start of saga)
        JsonNode payload = objectMapper.createObjectNode()
                .put("bookingId", bookingId)
                .put("customerId", "CUST-1")
                .put("flightId", "FL-1");
        String envelope = objectMapper.createObjectNode()
                .put("correlationId", "corr-e2e-1")
                .set("payload", payload)
                .toString();

        kafkaTemplate.send("booking.created.v1", bookingId, envelope).get();

        // wait for final deducted event (signals saga completion)
        String deducted = deductedQueue.poll(15, java.util.concurrent.TimeUnit.SECONDS);
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
    }

    // Simulators moved to top-level SimulatorsTestConfig
}
