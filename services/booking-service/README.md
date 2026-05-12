# booking-service

Skeleton for booking-service (Java 17, Spring Boot 3, Maven, Spring Kafka, MyBatis)

Structure:
- src/main/java/
  - domain/
  - application/
  - infrastructure/
  - interfaces/

Endpoints:
- POST /bookings - create booking (PENDING)

Kafka topics (subscribe/publish):
- publish: booking.created.v1, booking.confirmed.v1, booking.cancelled.v1
- consume: seat.reserved.v1, loyalty.points.reserved.v1, payment.completed.v1, payment.failed.v1

DB: Oracle, schema BOOKING_SCHEMA

Run:
- Use docker-compose in infra/ to start Kafka and Oracle
