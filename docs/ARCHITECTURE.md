# Architecture Overview

Microservice architecture for Airline Booking & Loyalty Platform.

Services:
- booking-service (BOOKING_SCHEMA)
- loyalty-service (LOYALTY_SCHEMA)
- payment-service (PAYMENT_SCHEMA)
- notification-service (NOTIFICATION_SCHEMA)
- seat-service (SEAT_SCHEMA)

Descriptions:
- booking-service: booking lifecycle, PENDING -> CONFIRMED -> CANCELLED, uses BOOKING_SCHEMA.
- loyalty-service: manages loyalty accounts, reserve/deduct/release points, uses LOYALTY_SCHEMA.
- payment-service: payment processing, integrates with payment gateway, uses PAYMENT_SCHEMA.
- seat-service: manages seat inventory and reservations, publishes seat.reserved.v1 and seat.released.v1, uses SEAT_SCHEMA.
- notification-service: sends emails/SMS for booking confirmations and cancellations; optional audit table in NOTIFICATION_SCHEMA.

Saga approach: choreography (event-only). Services publish/subscribe to Kafka topics and implement compensation on failure.

DB per service: Oracle XE container with separate schemas per service (see infra/oracle/init-scripts/create_schemas.sql)

Infra: kafka, zookeeper, oracle (dev container). docker-compose.yml provided in infra/
