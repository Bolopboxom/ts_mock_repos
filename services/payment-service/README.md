# payment-service

Skeleton for payment-service (Java 17, Spring Boot 3, Maven, Spring Kafka, MyBatis)

Structure:
- src/main/java/
  - domain/
  - application/
  - infrastructure/
  - interfaces/

Kafka topics:
- publish: payment.completed.v1, payment.failed.v1
- consume: booking.created.v1, loyalty.points.reserved.v1

DB: Oracle, schema PAYMENT_SCHEMA
