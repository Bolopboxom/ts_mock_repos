package com.airline.booking.infrastructure.kafka.consumer;

import com.airline.config.KafkaConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

@TestConfiguration(proxyBeanMethods = false)
@Import(KafkaConfig.class)
public class SimulatorsTestConfig {

    @Bean
    public BlockingQueue<String> deductedQueue() {
        return new ArrayBlockingQueue<>(10);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Object seatSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        return new Object() {
            @KafkaListener(topics = "booking.created.v1", groupId = "sim-seat-group")
            public void onBookingCreated(String message) throws Exception {
                System.out.println("[sim-seat] Received booking.created.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("seatId", "S1");
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-seat] Publishing seat.reserved.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("seat.reserved.v1", bookingId, outEnv);
            }
        };
    }

    @Bean
    public Object loyaltySimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper, BlockingQueue<String> deductedQueue) {
        return new Object() {
            @KafkaListener(topics = "seat.reserved.v1", groupId = "sim-loyalty-group")
            public void onSeatReserved(String message) throws Exception {
                System.out.println("[sim-loyalty] Received seat.reserved.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("pointsReserved", 100);
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-loyalty] Publishing loyalty.points.reserved.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("loyalty.points.reserved.v1", bookingId, outEnv);
            }

            @KafkaListener(topics = "booking.confirmed.v1", groupId = "sim-loyalty-group")
            public void onBookingConfirmed(String message) throws Exception {
                System.out.println("[sim-loyalty] Received booking.confirmed.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("pointsDeducted", 100);
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-loyalty] Publishing loyalty.points.deducted.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("loyalty.points.deducted.v1", bookingId, outEnv);
                boolean offered = deductedQueue.offer(outEnv);
                System.out.println("[sim-loyalty] deductedQueue.offer returned=" + offered + " for bookingId=" + bookingId);
            }
        };
    }

    @Bean
    public Object paymentSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        return new Object() {
            @KafkaListener(topics = "loyalty.points.reserved.v1", groupId = "sim-payment-group")
            public void onPointsReserved(String message) throws Exception {
                System.out.println("[sim-payment] Received loyalty.points.reserved.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("transactionId", "txn-e2e-1");
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-payment] Publishing payment.completed.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("payment.completed.v1", bookingId, outEnv);
            }
        };
    }

    @Bean
    public java.util.concurrent.CountDownLatch testLatch() {
        return new java.util.concurrent.CountDownLatch(1);
    }
}
