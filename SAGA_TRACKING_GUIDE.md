# Saga Tracking - Transaction Monitoring Guide

## Overview

This system implements **distributed saga tracking** to monitor the progress of transactions across microservices. Each booking request creates a saga that flows through multiple services, and you can track its progress in real-time using a unique `transactionId` (correlationId).

## How It Works

### 1. Transaction ID (Correlation ID)

Every booking request gets a unique `transactionId` (also called `correlationId`) that propagates through the entire saga flow:

```
POST /bookings
Header: X-Correlation-ID: custom-correlation-123

OR (auto-generated if not provided):
correlationId: corr-abc12345
```

### 2. Saga Flow Tracking

The system automatically tracks each step as the saga progresses through services:

```
booking-service → seat-service → loyalty-service → payment-service → booking-service (confirm)
       ↓              ↓               ↓                  ↓                   ↓
   booking.        seat.         loyalty.points.     payment.          booking.
   created.v1    reserved.v1      reserved.v1      completed.v1      confirmed.v1
```

Each event is recorded with:
- **Service name**: Which service processed the step
- **Event type**: Which event was published
- **Status**: SUCCESS, FAILED, or COMPENSATED
- **Timestamp**: When the step occurred
- **Details**: Additional information about the step

### 3. Saga States

- **IN_PROGRESS**: Saga is currently executing
- **COMPLETED**: All steps completed successfully
- **COMPENSATING**: Compensation (rollback) in progress
- **FAILED**: Saga failed and compensation completed

## API Endpoints

### Get Saga Status by Transaction ID

```bash
GET /saga-tracking/transaction/{transactionId}

# Example
curl http://localhost:8081/saga-tracking/transaction/corr-e2e-123
```

**Response:**
```json
{
  "transactionId": "corr-e2e-123",
  "bookingId": "BKG-abc12345",
  "status": "COMPLETED",
  "startedAt": "2024-01-15T10:30:00",
  "completedAt": "2024-01-15T10:30:05",
  "progressPercentage": 100.0,
  "currentStep": "booking-service:booking.confirmed.v1",
  "steps": [
    {
      "serviceName": "booking-service",
      "eventType": "booking.created.v1",
      "status": "SUCCESS",
      "timestamp": "2024-01-15T10:30:00",
      "details": "Booking created: BKG-abc12345"
    },
    {
      "serviceName": "seat-service",
      "eventType": "seat.reserved.v1",
      "status": "SUCCESS",
      "timestamp": "2024-01-15T10:30:01",
      "details": "Seat reserved for booking: BKG-abc12345"
    },
    {
      "serviceName": "loyalty-service",
      "eventType": "loyalty.points.reserved.v1",
      "status": "SUCCESS",
      "timestamp": "2024-01-15T10:30:02",
      "details": "Loyalty points reserved for booking: BKG-abc12345"
    },
    {
      "serviceName": "payment-service",
      "eventType": "payment.completed.v1",
      "status": "SUCCESS",
      "timestamp": "2024-01-15T10:30:03",
      "details": "Payment successful for booking: BKG-abc12345"
    },
    {
      "serviceName": "booking-service",
      "eventType": "booking.confirmed.v1",
      "status": "SUCCESS",
      "timestamp": "2024-01-15T10:30:04",
      "details": "Booking confirmed with ticket: TKT-c12345"
    }
  ]
}
```

### Get Saga Status by Booking ID

```bash
GET /saga-tracking/booking/{bookingId}

# Example
curl http://localhost:8081/saga-tracking/booking/BKG-abc12345
```

### Get All In-Progress Sagas

```bash
GET /saga-tracking/in-progress

# Example - useful for monitoring dashboard
curl http://localhost:8081/saga-tracking/in-progress
```

### Get All Sagas

```bash
GET /saga-tracking/all

# Example
curl http://localhost:8081/saga-tracking/all
```

## Usage Examples

### 1. Create Booking and Track Progress

```bash
# Step 1: Create booking with custom correlation ID
curl -X POST http://localhost:8081/bookings \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: my-transaction-001" \
  -d '{
    "customerId": "CUST001",
    "flightId": "FL123",
    "seatClass": "BUSINESS",
    "passengerCount": 2,
    "loyaltyPointsToUse": 1000
  }'

# Response: BKG-xyz789

# Step 2: Track saga progress
curl http://localhost:8081/saga-tracking/transaction/my-transaction-001

# Step 3: Track by booking ID
curl http://localhost:8081/saga-tracking/booking/BKG-xyz789
```

### 2. Monitor Compensation Flow

```bash
# For a payment failure scenario, the saga will show compensation steps:

{
  "transactionId": "corr-comp-456",
  "bookingId": "BKG-fail123",
  "status": "FAILED",
  "progressPercentage": 71.4,
  "steps": [
    {
      "serviceName": "booking-service",
      "eventType": "booking.created.v1",
      "status": "SUCCESS",
      "details": "Booking created: BKG-fail123"
    },
    {
      "serviceName": "seat-service",
      "eventType": "seat.reserved.v1",
      "status": "SUCCESS",
      "details": "Seat reserved for booking: BKG-fail123"
    },
    {
      "serviceName": "loyalty-service",
      "eventType": "loyalty.points.reserved.v1",
      "status": "SUCCESS",
      "details": "Loyalty points reserved"
    },
    {
      "serviceName": "payment-service",
      "eventType": "payment.failed.v1",
      "status": "FAILED",
      "details": "Payment failed: INSUFFICIENT_FUNDS"
    },
    {
      "serviceName": "loyalty-service",
      "eventType": "loyalty.points.released.v1",
      "status": "COMPENSATED",
      "details": "Loyalty points released for booking: BKG-fail123"
    },
    {
      "serviceName": "seat-service",
      "eventType": "seat.released.v1",
      "status": "COMPENSATED",
      "details": "Seat released for booking: BKG-fail123"
    },
    {
      "serviceName": "booking-service",
      "eventType": "booking.cancelled.v1",
      "status": "COMPENSATED",
      "details": "Booking cancelled due to payment failure"
    }
  ]
}
```

## Benefits

1. **Real-time Monitoring**: Track saga progress in real-time
2. **Debugging**: Easily identify which service failed and why
3. **Audit Trail**: Complete history of all saga steps
4. **Customer Support**: Quickly check booking status without checking multiple services
5. **Performance Analysis**: Measure saga execution time and identify bottlenecks
6. **Compensation Visibility**: See exactly which compensation steps were executed

## Implementation Details

### Storage
- Currently using **in-memory storage** (ConcurrentHashMap)
- For production: Migrate to persistent storage (database or Redis)

### Components
- **SagaTracker**: Domain model representing saga state
- **SagaTrackerRepository**: Repository interface for tracking data
- **SagaTrackingService**: Business logic for tracking operations
- **SagaTrackingController**: REST API endpoints

### Integration Points
All Kafka consumers automatically record saga steps:
- `CreateBookingUseCase`: Initializes saga
- `PaymentCompletedConsumer`: Records payment success and completes saga
- `PaymentFailedConsumer`: Records failure and compensation
- Test simulators: Also record steps for testing

## Future Enhancements

1. **Persistent Storage**: Migrate to database for saga history
2. **WebSocket Support**: Real-time push notifications for saga updates
3. **Saga Timeout Detection**: Alert on sagas that take too long
4. **Retry Mechanism**: Automatic retry for transient failures
5. **Dashboard UI**: Visual representation of saga flows
6. **Metrics**: Prometheus metrics for saga success/failure rates
7. **Correlation with Distributed Tracing**: Integrate with Jaeger/Zipkin
