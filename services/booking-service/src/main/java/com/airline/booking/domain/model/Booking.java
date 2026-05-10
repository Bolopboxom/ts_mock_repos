package com.airline.booking.domain.model;

import java.time.Instant;

public class Booking {
    public enum Status {PENDING, RESERVED, PAYMENT_PROCESSING, CONFIRMED, FAILED, CANCELLED}

    private final String bookingId;
    private final String customerId;
    private final String flightId;
    private Status status;
    private Instant createdAt;
    private String ticketNumber;

    public Booking(String bookingId, String customerId, String flightId) {
        this.bookingId = bookingId;
        this.customerId = customerId;
        this.flightId = flightId;
        this.status = Status.PENDING;
        this.createdAt = Instant.now();
    }

    public String getBookingId() { return bookingId; }
    public String getCustomerId() { return customerId; }
    public String getFlightId() { return flightId; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public Instant getCreatedAt() { return createdAt; }
    public String getTicketNumber() { return ticketNumber; }
    public void setTicketNumber(String ticketNumber) { this.ticketNumber = ticketNumber; }
}
