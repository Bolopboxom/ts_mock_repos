Common utilities used across services (demo):

- EventEnvelope.java: simple envelope class for events
- CorrelationIdFilter.java: web filter to propagate correlation id (TODO)
- IdempotencyStore.java: interface to implement dedup store (in-memory/Redis)

These are for demo only and not production-ready.
