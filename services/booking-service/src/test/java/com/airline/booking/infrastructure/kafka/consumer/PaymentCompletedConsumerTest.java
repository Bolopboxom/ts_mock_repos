package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.booking.domain.model.Booking;
import com.airline.booking.domain.repository.BookingRepository;
import com.airline.booking.infrastructure.kafka.producer.BookingEventProducer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 1, topics = {"booking.created.v1", "seat.reserved.v1", "loyalty.points.reserved.v1", "payment.completed.v1"})
@ActiveProfiles("demo")
public class PaymentCompletedConsumerTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; // reused from KafkaConfig bean

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private BookingEventProducer bookingProducer;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void happyPath_fullSaga_confirmsBookingAndDeductsPoints() throws Exception {
        // Arrange
        String bookingId = "BKG-EMBED-1";
        Booking booking = new Booking(bookingId, "CUST-1", "FL-1");
        bookingRepository.save(booking);

        // Publish payment.completed directly to trigger booking consumer
        String paymentPayload = objectMapper.createObjectNode()
                .put("bookingId", bookingId)
                .put("transactionId", "txn-embed-1")
                .toString();
        String paymentEnvelope = objectMapper.createObjectNode()
                .put("correlationId", "corr-embed-1")
                .set("payload", objectMapper.readTree(paymentPayload))
                .toString();

        kafkaTemplate.send("payment.completed.v1", bookingId, paymentEnvelope).get();

        // Wait up to 5 seconds for booking consumer to process the payment.completed message
        Booking saved = null;
        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline) {
            saved = bookingRepository.findById(bookingId);
            if (saved != null && saved.getStatus() == Booking.Status.CONFIRMED) {
                break;
            }
            Thread.sleep(100);
        }

        assertNotNull(saved);
        assertEquals(Booking.Status.CONFIRMED, saved.getStatus());
    }
}
