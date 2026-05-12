# seat-service

Skeleton for seat-service (Java 17, Spring Boot 3, Maven, Spring Kafka, MyBatis)

Responsibility: manage seat inventory and reservations.

Structure (suggested):
- src/main/java/com/airline/seat/
  - domain/
  - application/
  - infrastructure/
  - interfaces/

Kafka topics:
- publish: seat.reserved.v1, seat.released.v1
- consume: booking.created.v1, payment.failed.v1

DB: Oracle, schema SEAT_SCHEMA (optional)
