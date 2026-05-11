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
    public BlockingQueue<String> releasedPointsQueue() {
        return new ArrayBlockingQueue<>(10);
    }

    @Bean
    public BlockingQueue<String> releasedSeatQueue() {
        return new ArrayBlockingQueue<>(10);
    }

    @Bean
    public BlockingQueue<String> cancelledQueue() {
        return new ArrayBlockingQueue<>(10);
    }

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }

    @Bean
    public Object paymentSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper) {
        return new Object() {
            @KafkaListener(topics = "loyalty.points.reserved.v1", groupId = "sim-payment-points-reserved-group")
            public void onPointsReserved(String message) throws Exception {
                System.out.println("[sim-payment] Received loyalty.points.reserved.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("paymentId", "PAY-" + bookingId).put("amount", 500);
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-payment] Publishing payment.completed.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("payment.completed.v1", bookingId, outEnv);
            }
        };
    }

    @Bean
    public Object loyaltySimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper, BlockingQueue<String> deductedQueue, BlockingQueue<String> releasedPointsQueue) {
        return new Object() {
            @KafkaListener(topics = "seat.reserved.v1", groupId = "sim-loyalty-seat-reserved-group")
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

            @KafkaListener(topics = "booking.confirmed.v1", groupId = "sim-loyalty-booking-confirmed-group")
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
                deductedQueue.put(outEnv);
                System.out.println("[sim-loyalty] deductedQueue.put completed for bookingId=" + bookingId);
            }

            // compensation: payment failed -> release points
            @KafkaListener(topics = "payment.failed.v1", groupId = "sim-loyalty-payment-failed-group")
            public void onPaymentFailed(String message) throws Exception {
                System.out.println("[sim-loyalty] Received payment.failed.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("pointsReleased", 100);
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-loyalty] Publishing loyalty.points.released.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("loyalty.points.released.v1", bookingId, outEnv);
                releasedPointsQueue.put(outEnv);
                System.out.println("[sim-loyalty] releasedPointsQueue.put completed for bookingId=" + bookingId);
            }
        };
    }

    @Bean
    public Object seatSimulator(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper mapper, BlockingQueue<String> releasedSeatQueue, BlockingQueue<String> cancelledQueue) {
        return new Object() {
            @KafkaListener(topics = "booking.created.v1", groupId = "sim-seat-booking-created-group")
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

            // compensation: points released -> release seat
            @KafkaListener(topics = "loyalty.points.released.v1", groupId = "sim-seat-points-released-group")
            public void onPointsReleased(String message) throws Exception {
                System.out.println("[sim-seat] Received loyalty.points.released.v1: " + message);
                JsonNode node = mapper.readTree(message);
                JsonNode payload = node.get("payload");
                String bookingId = payload.get("bookingId").asText();
                JsonNode outPayload = mapper.createObjectNode().put("bookingId", bookingId).put("seatReleased", true);
                String outEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", outPayload).toString();
                System.out.println("[sim-seat] Publishing seat.released.v1 for bookingId=" + bookingId + " payload=" + outEnv);
                kafkaTemplate.send("seat.released.v1", bookingId, outEnv);
                releasedSeatQueue.put(outEnv);
                System.out.println("[sim-seat] releasedSeatQueue.put completed for bookingId=" + bookingId);

                // final compensation: publish booking.cancelled.v1
                JsonNode cancelPayload = mapper.createObjectNode().put("bookingId", bookingId).put("reason", "PAYMENT_FAILED");
                String cancelEnv = mapper.createObjectNode().put("correlationId", node.has("correlationId")?node.get("correlationId").asText():null)
                        .set("payload", cancelPayload).toString();
                System.out.println("[sim-seat] Publishing booking.cancelled.v1 for bookingId=" + bookingId + " payload=" + cancelEnv);
                kafkaTemplate.send("booking.cancelled.v1", bookingId, cancelEnv);
                cancelledQueue.put(cancelEnv);
                System.out.println("[sim-seat] cancelledQueue.put completed for bookingId=" + bookingId);
            }
        };
    }

}
