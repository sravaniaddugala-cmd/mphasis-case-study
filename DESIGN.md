# Design Note — Idempotent Background Transaction Processing

## Architecture

Spring Boot REST controllers accept transaction requests and management queries. `TransactionProcessingService` validates requests, detects duplicate/idempotency conflicts, sequences records by account, applies credit/debit/reversal rules, manages retry attempts, and emits structured logs. `TransactionRecordRepository` is an in-memory repository abstraction that indexes canonical transactions and records each observed delivery separately. Management APIs support topic/year filtering and exact record lookup, with one record per page.

`application.yaml` externalizes topic names, retry configuration, downstream client URLs/timeouts, and credential placeholders. The current assessment implementation does not yet connect to Kafka, PostgreSQL, or the configured client URLs; the in-memory behavior is an executable model of the processing logic.

## Idempotency design

- The producer supplies a stable `messageId` and must reuse it for redelivery of the same logical event. For compatibility, `requestId` is used as a fallback if `messageId` is omitted.
- `receivedAt` and `recordId` describe each observed delivery and are not part of the idempotency key.
- Same key + same business payload becomes `DUPLICATE`; a delivery-only audit record is retained and no ledger balance is changed again.
- Same key + different payload becomes `REJECTED` and is logged as an idempotency collision.
- `transactionId` is independently guarded to prevent a new message ID from applying the same business transaction twice.
- In production, enforce uniqueness in the database and commit the idempotency decision and ledger side effect atomically.

## Duplicate detection logic

Check the stable message ID first, then the business transaction ID. Compare the business payload for collisions. Record duplicate deliveries separately with their own `recordId`/`receivedAt` and a `duplicateOfRecordId` reference to the original record. Duplicate delivery is an expected result rather than an exception; conflicting reuse of a key is rejected.

## Out-of-order handling strategy

Maintain a per-account sequence index and expected next sequence. A future sequence is marked `PENDING` until the missing sequence arrives. Process contiguous records in order. A failed expected sequence blocks later records rather than silently changing ledger order. Sequence number starts at 1 per account in this demo; production should define reset/reconciliation semantics explicitly.

## Retry and failure handling

Allow one initial attempt plus up to three retries in a retry cycle. Simulated transient failures occur before the ledger side effect, so retries cannot double-post a balance. Exhaustion produces `FAILED`; manual retry starts a new cycle only for failed transactions. Batch processing isolates records so a bad item does not stop later items. The configured one-hour replay interval is a future integration setting; a durable scheduler/replay worker is not wired into this version.

## Exception strategy

`ApiExceptionHandler` provides consistent RFC-style Spring `ProblemDetail` responses for malformed requests, validation errors, duplicate-message exceptions, sequence conflicts, retry restrictions/exhaustion, invalid transactions, missing records, and unexpected server errors. Unexpected errors are logged server-side with request method/path and return a generic message to avoid leaking internals. Business-level rejected/duplicate deliveries are preserved as auditable records rather than converted to HTTP errors.

## Observability approach

Structured application logs include message ID, record ID, transaction ID, account, attempt/retry count, outcome and reason. `/api/summary` reports processing-state counts, processed count and duplicate count. Production should add Micrometer counters/timers, correlation IDs, distributed tracing, dashboards, alerts for retry exhaustion/DLQ volume, and audit retention controls.


## Production hardening recommendations

1. Use real Kafka consumers/producers with consumer groups, explicit acknowledgment strategy, stable producer event IDs, schema validation, and dead-letter topics.
2. Persist audit records, idempotency keys, ledger entries and sequence checkpoints in PostgreSQL with unique constraints and transactions.
3. Make ledger posting and idempotency decisions atomic; consider an outbox for downstream publication.
4. Implement a durable scheduled replay worker with exponential backoff, bounded retries, backpressure and poison-message isolation.
5. Use locking/optimistic concurrency or account-key partitioning to prevent multi-instance sequence races.
6. Wire authenticated downstream clients with TLS, secret-manager credentials, connection/read timeouts, circuit breakers and safe logging.
7. Add authentication/authorization, request size/rate limits, input/schema versioning, currency precision, reconciliation, reversal policy and retention rules.
8. Add unit/integration/contract tests, Testcontainers for Kafka/PostgreSQL, metrics, tracing and operational runbooks.
