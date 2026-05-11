# Airline Booking & Loyalty Platform — SAGA (Choreography)

Demo repository that demonstrates a production-style microservice architecture using Domain-Driven Design (DDD) principles and a Choreography SAGA implemented with Kafka. The focus is on the customer booking flow and distributed compensation handling (payment failure → release points → release seat → cancel booking).

## High-level overview
- Architecture: Event-driven microservices (Kafka) using Saga (choreography).
- Languages / Tech: Java 17, Spring Boot 3, Spring Kafka, Maven.
- DB strategy: DB-per-service (Oracle schemas used in infra for demo).
- Event envelope: shared `EventEnvelope` in `libs/common` (propagates eventId, correlationId, timestamp, payload).

## Services (in `services/`)
- booking-service: initiates booking flow (POST /bookings). Publishes `booking.created.v1`, consumes payment events and publishes `booking.confirmed.v1` / `booking.cancelled.v1`.
- seat-service: reserves/releases seats. Publishes `seat.reserved.v1` and `seat.released.v1`.
- loyalty-service: reserve/deduct/release loyalty points. Publishes `loyalty.points.*.v1` events.
- payment-service: processes payments (demo). Publishes `payment.completed.v1` or `payment.failed.v1`.
- notification-service: sends notifications for confirmations/cancellations.

Shared libs
- `libs/events/`: JSON schemas for event contracts (v1).
- `libs/common/`: `EventEnvelope` and small utilities.

Infrastructure
- `infra/docker-compose.yml` provides Kafka, Zookeeper, Oracle XE (dev) and a Flyway container to run demo migrations under `infra/flyway`.
- Flyway migrations create demo Oracle schemas for each service.

Event flow (happy path)
1. Client POST /bookings → booking.created.v1
2. seat-service consumes booking.created.v1 → seat.reserved.v1
3. loyalty-service consumes seat.reserved.v1 → loyalty.points.reserved.v1
4. payment-service consumes loyalty.points.reserved.v1 → payment.completed.v1
5. booking-service consumes payment.completed.v1 → booking.confirmed.v1 → notification-service

Compensation flow (payment failure)
1. payment-service publishes payment.failed.v1 (demo: any bookingId ending with "fail" triggers failure)
2. loyalty-service consumes payment.failed.v1 → publishes loyalty.points.released.v1
3. seat-service or seat release consumer handles releasing seat → publishes seat.released.v1
4. booking-service consumes failure chain and publishes booking.cancelled.v1 and marks booking CANCELLED

Quick start (local dev)
1. Start infra: from `infra/` run Docker Compose:
   - powershell: docker compose up -d
2. Build services:
   - mvn clean install
3. Run services (IDE or java -jar) or run from IDE per module.
4. Create a booking (happy path): POST to booking-service `/bookings` with JSON body (see `services/booking-service/README.md`).
5. Simulate payment failure: create a bookingId that ends with `fail` or call booking POST then append `fail` to id when testing to trigger `payment.failed.v1` path.

Notes and next steps
- Persistence adapters are currently in-memory placeholders; convert to Oracle + MyBatis adapters to demonstrate DB-per-service in production.
- Add idempotency/dedup, retries and DLQ for robust consumer behavior.
- Transactional Outbox pattern is recommended for production (not fully implemented here) — see `libs/common` and design docs.

Contributing
- Work on `feat/*` branches and open PRs against `develop`.

License
- Internal demo (no license file provided).