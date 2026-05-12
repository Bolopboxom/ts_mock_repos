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
@EmbeddedKafka(partitions = 1, topics = {"booking.created.v1", "seat.reserved.v1", "loyalty.points.reserved.v1", "payment.failed.v1", "loyalty.points.released.v1", "seat.released.v1", "booking.cancelled.v1", "booking.confirmed.v1", "payment.completed.v1", "loyalty.points.deducted.v1"})
@Import(SimulatorsTestConfig.class)
@ActiveProfiles("local")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
public class FullSagaCompensationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private BlockingQueue<String> releasedPointsQueue;

    @Autowired
    private BlockingQueue<String> releasedSeatQueue;

    @Autowired
    private BlockingQueue<String> cancelledQueue;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private void waitForListenerContainers(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            boolean allRunning = true;
            boolean allAssigned = true;
            for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
                if (!container.isRunning()) { allRunning = false; break; }
                if (container instanceof org.springframework.kafka.listener.KafkaMessageListenerContainer) {
                    org.springframework.kafka.listener.KafkaMessageListenerContainer<?,?> kafkaContainer = (org.springframework.kafka.listener.KafkaMessageListenerContainer<?,?>) container;
                    try {
                        java.util.Collection<org.apache.kafka.common.TopicPartition> assigned = kafkaContainer.getAssignedPartitions();
                        if (assigned == null || assigned.isEmpty()) { allAssigned = false; break; }
                    } catch (UnsupportedOperationException ignored) {
                        // ignore
                    }
                }
            }
            if (allRunning && allAssigned) { Thread.sleep(500); return; }
            Thread.sleep(100);
        }
    }

    @Test
    void compensationPath_paymentFailed_releases_points_releases_seat_and_cancels_booking() throws Exception {
        String bookingId = "BKG-COMP-TEST-1";
        Booking booking = new Booking(bookingId, "CUST-X", "FL-X");
        bookingRepository.save(booking);

        // ensure listeners are started and partitions assigned
        waitForListenerContainers(15000);
        Thread.sleep(1000); // allow group stabilization

        // publish booking.created.v1 to start the saga
        JsonNode payload = objectMapper.createObjectNode()
                .put("bookingId", bookingId)
                .put("customerId", "CUST-X")
                .put("flightId", "FL-X");
        String envelope = objectMapper.createObjectNode()
                .put("correlationId", "corr-comp-test-1")
                .set("payload", payload)
                .toString();
        kafkaTemplate.send("booking.created.v1", bookingId, envelope).get();

        // wait for saga to progress (seat.reserved -> loyalty.points.reserved)
        Thread.sleep(2000);

        // publish payment.failed.v1 to trigger compensation
        JsonNode failPayload = objectMapper.createObjectNode().put("bookingId", bookingId).put("reason", "SIMULATED_FAILURE");
        String failEnv = objectMapper.createObjectNode().put("correlationId", "corr-comp-test-1").set("payload", failPayload).toString();
        kafkaTemplate.send("payment.failed.v1", bookingId, failEnv).get();
        System.out.println("[TEST] Published payment.failed.v1 for bookingId=" + bookingId);

        // expect loyalty.points.released.v1
        String releasedPoints = releasedPointsQueue.poll(20, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(releasedPoints != null && releasedPoints.contains(bookingId), "Expected loyalty.points.released.v1 for bookingId");

        // expect seat.released.v1
        String releasedSeat = releasedSeatQueue.poll(20, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(releasedSeat != null && releasedSeat.contains(bookingId), "Expected seat.released.v1 for bookingId");

        // expect booking.cancelled.v1
        String cancelled = cancelledQueue.poll(20, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(cancelled != null && cancelled.contains(bookingId), "Expected booking.cancelled.v1 for bookingId");
        System.out.println("[TEST] Received booking.cancelled.v1: " + cancelled);

        // booking should be CANCELLED in repository
        long deadline = System.currentTimeMillis() + 5000; // increased timeout
        boolean cancelledStatus = false;
        while (System.currentTimeMillis() < deadline) {
            Booking saved = bookingRepository.findById(bookingId);
            System.out.println("[TEST] Checking booking status: " + (saved != null ? saved.getStatus() : "null"));
            if (saved != null && saved.getStatus() == Booking.Status.CANCELLED) { cancelledStatus = true; break; }
            Thread.sleep(100);
        }
        assertTrue(cancelledStatus, "Booking should be CANCELLED after compensation path");
    }
}
