# Airline Booking & Loyalty Platform — SAGA (Choreography)

## Business Overview

### Domain Context
This system serves the **airline industry**, managing the complete booking lifecycle for flight reservations with integrated loyalty program support.

### User Types
The platform is designed to support three primary user categories:
- **CUSTOMER**: Registered users with loyalty accounts
- **NON-CUSTOMER**: Guest users without loyalty benefits
- **CORPORATE**: Business accounts with enterprise-level booking capabilities

### Current Implementation: Customer Booking Flow

**Business Goals**:

Customers can:
- Book flight tickets with seat selection
- Apply loyalty points to reduce payment amount
- Complete payment for remaining balance
- Receive booking confirmation via email/notification

System capabilities:
- Ensures **data consistency** across distributed microservices
- Supports **automatic rollback** when payment fails
- Operates on **event-driven architecture** for scalability
- Maintains **saga choreography** for distributed transaction management

**Flow Summary**:
```
Customer → Book Flight → Apply Loyalty Points → Process Payment → Confirm Booking → Send Notification
                                                         ↓ (if fails)
                              Release Points ← Release Seat ← Cancel Booking ← Notify Customer
```

---

## Technical Overview

A **production-style microservice architecture** demonstrating **Domain-Driven Design (DDD)** principles and **Choreography-based SAGA** pattern implemented with **Kafka**. The system handles customer booking flows and distributed compensation scenarios (payment failure → release points → release seat → cancel booking).

This platform features comprehensive test coverage across unit, integration, and end-to-end layers with proven stabilization techniques for event-driven architectures.

## High-level overview
- **Architecture**: Event-driven microservices with Saga choreography pattern
- **Languages / Tech**: Java 17, Spring Boot 3.2.0, Spring Kafka, Maven (multi-module)
- **DB strategy**: Database-per-service pattern with H2 in-memory databases (Oracle compatibility mode)
- **Event envelope**: Shared `EventEnvelope` in `libs/common` (propagates eventId, correlationId, timestamp, payload)
- **Testing**: 18 comprehensive tests (7 unit, 9 integration, 2 E2E) achieving ~70-80% coverage for booking-service
- **Codebase**: 52 Java source files across 5 microservices and 2 shared libraries

## Architecture & DDD Layers

This platform follows a **multi-module Maven architecture** with clear domain separation:

### Project Structure
```
ts_mock_repos/
├── libs/
│   ├── common/              # Shared utilities and EventEnvelope
│   └── events/              # Event schema contracts (JSON)
└── services/
    ├── booking-service/     # Booking aggregate & saga coordination
    ├── seat-service/        # Seat inventory management
    ├── loyalty-service/     # Loyalty points management
    ├── payment-service/     # Payment processing
    └── notification-service/# Customer notifications
```

### Domain Boundaries

Each service represents a **bounded context** with its own domain model:

| Service | Bounded Context | Core Domain Entities | Responsibilities |
|---------|----------------|---------------------|------------------|
| **booking-service** | Booking Management | `Booking`, `BookingStatus` | Saga orchestration, booking lifecycle |
| **seat-service** | Seat Inventory | `Seat`, `SeatReservation` | Seat allocation, reservation management |
| **loyalty-service** | Loyalty Program | `LoyaltyAccount`, `PointsTransaction` | Points reserve/deduct/release |
| **payment-service** | Payment Processing | `Payment`, `PaymentStatus` | Payment execution, failure handling |
| **notification-service** | Notification | `Notification`, `NotificationChannel` | Email/SMS delivery |

### Shared Libraries

| Library | Purpose | Contents |
|---------|---------|----------|
| **libs/common** | Cross-cutting concerns | `EventEnvelope`, correlation utilities, base interfaces |
| **libs/events** | Event contracts | JSON schemas for all domain events (.v1) |

Each service follows **Domain-Driven Design** principles with clear separation of concerns:

### DDD Layer Structure
```
service/
├── domain/           # Core business entities and domain logic
│   ├── model/       # Aggregates, Entities, Value Objects
│   └── repository/  # Repository interfaces (ports)
├── application/     # Use cases / Application services
│   └── usecase/    # Business orchestration (e.g., CreateBookingUseCase)
├── infrastructure/  # Technical implementations
│   ├── persistence/ # Repository implementations (adapters)
│   └── kafka/      # Kafka producers and consumers
└── interfaces/      # External interfaces
    └── rest/       # REST controllers (API layer)
```

### Key Design Decisions
- **Ports & Adapters**: Domain defines interfaces; infrastructure provides implementations
- **Event Sourcing**: All state changes communicated via Kafka events
- **Saga Choreography**: No orchestrator; services react to events autonomously
- **Idempotency**: Each consumer tracks processed eventIds (correlation-based deduplication)
- **Test Isolation**: Distinct Kafka consumer `groupId` per listener to prevent rebalance conflicts

## Services (in `services/`)

### booking-service ✅ **70-80% Test Coverage**
- **Responsibilities**: Orchestrates booking lifecycle via REST API; manages saga coordination
- **Endpoints**: `POST /bookings` (returns 202 Accepted with bookingId)
- **Publishes**: `booking.created.v1`, `booking.confirmed.v1`, `booking.cancelled.v1`
- **Consumes**: `payment.completed.v1`, `payment.failed.v1`
- **Test Suite** (18 total):
  - **Unit tests** (7): `CreateBookingUseCaseTest` - validates business logic with mocked dependencies
  - **Integration tests** (9): `BookingControllerTest` - verifies REST API contract using @WebMvcTest
  - **E2E tests** (2): `FullSagaE2ETest` (happy path), `FullSagaCompensationTest` (compensation flow)

### seat-service
- **Responsibilities**: Manages seat inventory; handles reservation and release operations
- **Publishes**: `seat.reserved.v1`, `seat.released.v1`
- **Consumes**: `booking.created.v1`, `payment.failed.v1` (triggers compensation)
- **Testing**: Currently validated through E2E test simulators

### loyalty-service
- **Responsibilities**: Manages customer loyalty points lifecycle (reserve/deduct/release)
- **Publishes**: `loyalty.points.reserved.v1`, `loyalty.points.deducted.v1`, `loyalty.points.released.v1`
- **Consumes**: `seat.reserved.v1`, `booking.confirmed.v1`, `payment.failed.v1`
- **Testing**: Currently validated through E2E test simulators

### payment-service
- **Responsibilities**: Processes payment transactions; initiates compensation on failure
- **Publishes**: `payment.completed.v1`, `payment.failed.v1`
- **Consumes**: `loyalty.points.reserved.v1`
- **Demo behavior**: BookingIds ending with "fail" trigger `payment.failed.v1` for testing compensation flows
- **Testing**: Currently validated through E2E test simulators

### notification-service
- **Responsibilities**: Delivers customer notifications for booking lifecycle events
- **Consumes**: `booking.confirmed.v1`, `booking.cancelled.v1`
- **Testing**: Currently validated through E2E test simulators

### Shared libs
- **`libs/events/`**: JSON schemas for event contracts (v1) - ensures consistent event structure across all services
- **`libs/common/`**: `EventEnvelope` wrapper and shared utilities for event handling and correlation

### Infrastructure
- **`infra/docker-compose.yml`**: Complete local development stack with Kafka and Zookeeper
- **Database**: H2 in-memory databases per service (configured with Oracle compatibility mode)
- Each service maintains isolated in-memory schema: `bookingdb`, `seatdb`, `loyaltydb`, `paymentdb`, `notificationdb`

## Test Strategy & Coverage

This project implements a **comprehensive testing strategy** following the test pyramid approach with 18 tests achieving ~70-80% coverage for booking-service.

### Test Summary

| Test Type | Count | Framework | Coverage |
|-----------|-------|-----------|----------|
| **Unit Tests** | 7 | JUnit 5 + Mockito | Business logic isolation |
| **Integration Tests** | 9 | @WebMvcTest + MockMvc | REST API contracts |
| **E2E Tests** | 2 | @SpringBootTest + @EmbeddedKafka | Full saga flows |

### Quick Test Commands

```powershell
# Run all tests
mvn test

# Run specific service tests
cd services/booking-service && mvn test

# Run specific test category
mvn test -Dtest=CreateBookingUseCaseTest      # Unit tests
mvn test -Dtest=BookingControllerTest         # Integration tests
mvn test -Dtest=FullSaga*                     # E2E tests
```

### Test Results
```
Tests run: 18, Failures: 0, Errors: 0, Skipped: 0
├── CreateBookingUseCaseTest:        7 tests (1.2s)
├── BookingControllerTest:           9 tests (0.5s)
├── FullSagaE2ETest:                 1 test  (3.6s)
└── FullSagaCompensationTest:        1 test  (7.0s)
```

**For detailed testing patterns, test simulators, stabilization techniques, and troubleshooting guide, see [TESTING_GUIDE.md](TESTING_GUIDE.md).**

## Event Flow

### Happy Path (Successful Booking)
```
Client
  │
  ├─ POST /bookings
  │
  v
[booking-service]
  │─ booking.created.v1 ──────┐
                               │
                               v
                         [seat-service]
                               │─ seat.reserved.v1 ──────┐
                                                           │
                                                           v
                                                     [loyalty-service]
                                                           │─ loyalty.points.reserved.v1 ──┐
                                                                                            │
                                                                                            v
                                                                                      [payment-service]
                                                                                            │─ payment.completed.v1
                                                                                            │
                                                                                            v
[booking-service] ◄──────────────────────────────────────────────────────────────────────┘
  │─ booking.confirmed.v1 ──────┐
                                 │
                                 v
                          [loyalty-service]
                                 │─ loyalty.points.deducted.v1
                                 │
                                 v
                          [notification-service]
                                 │
                                 └─ Send confirmation email/SMS
```

**Event sequence**:
1. Client `POST /bookings` → **booking.created.v1**
2. seat-service reserves seat → **seat.reserved.v1**
3. loyalty-service reserves points → **loyalty.points.reserved.v1**
4. payment-service processes payment → **payment.completed.v1**
5. booking-service confirms booking → **booking.confirmed.v1**
6. loyalty-service deducts points → **loyalty.points.deducted.v1**
7. notification-service sends confirmation → Customer notified ✅

### Compensation Flow (Payment Failure)
```
[payment-service]
  │─ payment.failed.v1 ──────┐
                              │
                              v
                        [loyalty-service]
                              │─ loyalty.points.released.v1
                              │
                              v
                        [seat-service]
                              │─ seat.released.v1
                              │
                              v
[booking-service] ◄───────────┘
  │─ booking.cancelled.v1 ────┐
                               │
                               v
                         [notification-service]
                               │
                               └─ Send cancellation notice
```

**Compensation sequence**:
1. payment-service fails payment → **payment.failed.v1** (demo: bookingId ends with "fail")
2. loyalty-service releases reserved points → **loyalty.points.released.v1**
3. seat-service releases reserved seat → **seat.released.v1**
4. booking-service cancels booking → **booking.cancelled.v1** (status → `CANCELLED`)
5. notification-service sends cancellation → Customer notified ❌

### Event envelope structure
All events use the shared `EventEnvelope` wrapper:
```json
{
  "eventId": "evt-uuid-12345",
  "correlationId": "corr-uuid-67890",
  "timestamp": "2024-01-15T10:30:00Z",
  "payload": "{ ... service-specific JSON ... }"
}
```

## Quick start (local dev)

### Prerequisites
- Java 17 (OpenJDK or Oracle JDK)
- Maven 3.8+
- Docker Desktop (for Kafka and Zookeeper)

### 1. Start Infrastructure
From `infra/` directory:
```powershell
docker compose up -d
```

Services started:
- Kafka (port 9092)
- Zookeeper (port 2181)

### 2. Build Services
From project root:
```powershell
mvn clean install
```

### 3. Run Tests
```powershell
# All tests
mvn test

# Booking-service only
cd services/booking-service
mvn test

# Quick validation (unit + integration tests only, skip E2E)
mvn test -Dtest=CreateBookingUseCaseTest,BookingControllerTest
```

### 4. Run Services
Option A - From IDE:
- Run `BookingApplication.java` (booking-service)
- Run `SeatApplication.java` (seat-service)
- Run `LoyaltyApplication.java` (loyalty-service)
- Run `PaymentApplication.java` (payment-service)
- Run `NotificationApplication.java` (notification-service)

Option B - From terminal:
```powershell
# Each in separate terminal
cd services/booking-service; mvn spring-boot:run
cd services/seat-service; mvn spring-boot:run
cd services/loyalty-service; mvn spring-boot:run
cd services/payment-service; mvn spring-boot:run
cd services/notification-service; mvn spring-boot:run
```

### 5. Test the Saga

**Service Ports** (configured in `application-demo.yml`):
- booking-service: port 8081
- seat-service: port 8082
- loyalty-service: port 8083
- payment-service: port 8084
- notification-service: port 8085

**Create a successful booking** (happy path):
```powershell
curl -X POST http://localhost:8081/bookings `
  -H "Content-Type: application/json" `
  -H "X-Correlation-ID: test-corr-123" `
  -d '{
    "customerId": "CUST001",
    "flightId": "FL123",
    "seatClass": "BUSINESS",
    "passengerCount": 2,
    "loyaltyPointsToUse": 1000
  }'
```

**Response** (202 Accepted):
```json
{
  "bookingId": "BKG-abc123..."
}
```

**Trigger compensation flow** (payment failure):
```powershell
# Use bookingId ending with "fail" to trigger payment.failed.v1
curl -X POST http://localhost:8081/bookings `
  -H "Content-Type: application/json" `
  -d '{
    "customerId": "CUST002",
    "flightId": "FL456",
    "seatClass": "ECONOMY",
    "passengerCount": 1,
    "loyaltyPointsToUse": 500
  }'
# Then manually modify payment-service logic or wait for demo bookingId with "fail" suffix
```

### 6. Monitor Events
Check Kafka topics:
```powershell
# List topics
docker exec -it infra-kafka-1 kafka-topics --list --bootstrap-server localhost:9092

# Consume events (example)
docker exec -it infra-kafka-1 kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic booking.created.v1 `
  --from-beginning
```

## Key Technical Highlights

### 1. Domain-Driven Design (DDD)
- **Bounded contexts**: Each service maintains its own domain model (Booking, Seat, LoyaltyAccount, Payment)
- **Aggregates**: Well-defined aggregate roots with clear consistency boundaries
- **Ports & Adapters**: Domain layer remains independent of infrastructure concerns
- **Value Objects**: Immutable types such as `BookingId`, `CustomerId`, `FlightId`

### 2. Saga Choreography Pattern
- **Decentralized coordination**: Services react autonomously to domain events without central orchestrator
- **Event-driven flow**: Each service publishes and consumes domain events independently
- **Distributed compensation**: Automated rollback through reverse event chains
- **Correlation tracking**: `correlationId` propagates through entire saga lifecycle for traceability

### 3. Kafka Event Streaming
- **Topic versioning**: All topics use `.v1` suffix to support schema evolution
- **Event envelope pattern**: Standardized wrapper containing metadata (eventId, correlationId, timestamp)
- **Consumer isolation**: Distinct `groupId` per listener prevents consumer group rebalancing conflicts
- **Idempotency**: EventId-based deduplication strategy (production-ready pattern)

### 4. Test Architecture
- **Test pyramid compliance**: Balanced distribution with more unit tests (7), fewer E2E tests (2)
- **Test simulators**: Replace downstream services in E2E tests without mocking production code
- **BlockingQueue synchronization**: Deterministic assertions for asynchronous Kafka message flows
- **Stabilization patterns**: Partition verification, extended timeouts, post-startup delays for reliability

### 5. Production Readiness Considerations
- **Database-per-service**: Each service uses isolated H2 in-memory database (production-ready for Oracle migration)
- **Transactional outbox**: Pattern design available in `libs/common` for atomic DB + event publishing
- **Dead Letter Queues (DLQ)**: Strategy for handling poison messages and retry exhaustion
- **Circuit breakers**: Resilience patterns for downstream service failures
- **Distributed tracing**: OpenTelemetry/Jaeger integration ready for saga flow visibility

## Project Statistics
- **Total Java files**: 52
- **Services**: 5 (booking, seat, loyalty, payment, notification)
- **Shared libraries**: 2 (events, common)
- **Test coverage**: 18 tests, 0 failures
  - Unit: 7 tests (business logic)
  - Integration: 9 tests (REST API)
  - E2E: 2 tests (full saga flows)
- **Coverage**: ~70-80% for booking-service (domain, application, interfaces)
