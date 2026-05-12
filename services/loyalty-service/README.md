# loyalty-service

Skeleton for loyalty-service (Java 17, Spring Boot 3, Maven, Spring Kafka, MyBatis)

Structure:
- src/main/java/
  - domain/
  - application/
  - infrastructure/
  - interfaces/

Kafka topics (publish/consume):
- publish: loyalty.points.reserved.v1, loyalty.points.deducted.v1, loyalty.points.released.v1
- consume: booking.created.v1, payment.completed.v1, payment.failed.v1

DB: Oracle, schema LOYALTY_SCHEMA
