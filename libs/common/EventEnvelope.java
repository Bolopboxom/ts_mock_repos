package com.airline.common;

import java.time.Instant;
import java.util.UUID;

public class EventEnvelope {
    public String eventId;
    public String correlationId;
    public String traceId;
    public String timestamp;
    public String version;
    public Object payload;

    public EventEnvelope(String correlationId, Object payload) {
        this.eventId = UUID.randomUUID().toString();
        this.correlationId = correlationId;
        this.traceId = null;
        this.timestamp = Instant.now().toString();
        this.version = "v1";
        this.payload = payload;
    }
}
