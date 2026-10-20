# Idempotent Background Transaction Processing Service

Mphasis FDE L2 practical assessment implementation.

## Stack
- Java 21 (configured for your local JDK)
- Spring Boot 3.3.5
- Gradle
- REST API
- In-memory repository abstraction (no external database required)
- JUnit 5

## Key design decisions

1. **Stable message idempotency key:** `messageId` is supplied by the producer and must be reused when the same logical Kafka message is republished/redelivered. For backward compatibility with existing requests, it defaults to `requestId` when omitted. In a real Kafka consumer, populate it from a stable producer event ID or a message header; do not generate a new ID on each retry.
2. **Timestamps are separate from identity:** each observed delivery gets a unique `recordId` and a service-assigned `receivedAt`. A redelivery may have a different `receivedAt`, but must retain the same `messageId`. The repository records duplicate deliveries separately and points to the original `duplicateOfRecordId`. This supports audit inspection without repeating the business side effect.
3. **Payload/key collision protection:** if an existing `messageId` or `transactionId` is reused with a different business payload, the new delivery is `REJECTED` rather than treated as a valid duplicate.
4. **Transaction idempotency:** `transactionId` is also protected. A second message with a different `messageId` but the same already-accepted transaction ID cannot apply a second balance effect.
5. **State transition model:** the record validates allowed transitions. Main path: `RECEIVED -> PROCESSING -> PROCESSED`; out-of-order records use `PENDING`; transient failures use `RETRY_PENDING`; terminal failures use `FAILED`; invalid inputs/key collisions use `REJECTED`; duplicate deliveries use `DUPLICATE`.
6. **Out-of-order handling:** sequences start at 1 per account. A future sequence waits in `PENDING`; once the missing sequence arrives, contiguous transactions are processed in order. A failed expected sequence blocks later records until recovery.
7. **Retry behavior:** up to 3 retries after the initial attempt (maximum 4 attempts per cycle). `transientFailuresToSimulate` can be used to test retry success and exhaustion. No balance is changed on a simulated transient failure. Manual retry of a `FAILED` transaction starts a new retry cycle.
8. **Batch failure isolation:** each item is handled independently; validation errors are recorded as `REJECTED`, and later records in the batch continue processing.
9. **Management lookup:** each observed delivery has a unique `recordId`, `topicName`, and `receivedAt`. Search can filter by topic and calendar year and returns at most one record per page; exact lookup requires topic plus record ID.
10. **Observability:** structured logs include message ID, record ID, transaction ID, attempts and outcome. `/api/summary` reports counts by state and duplicate count.

## Run
Requires JDK 21 and Gradle 8.5 or newer. Buildship is already installed in your Eclipse according to your screenshot. This ZIP does not include the Gradle Wrapper, so use an installed Gradle once, or generate the wrapper with the command shown below.

```bat
gradle wrapper --gradle-version 8.10.2
gradlew.bat clean test
gradlew.bat bootRun
```

Application listens on `http://localhost:8080`.

## API examples

### Submit a credit
Use a stable `messageId`; when the same logical Kafka event is redelivered, send the same ID again.

```bash
curl -X POST http://localhost:8080/api/transactions \
  -H "Content-Type: application/json" \
  -d '{
    "messageId":"MSG-2001",
    "transactionId":"TXN-2001",
    "requestId":"REQ-1",
    "accountId":"ACC-201",
    "transactionType":"CREDIT",
    "amount":100,
    "currency":"USD",
    "sequenceNumber":1,
    "topicName":"payments.inbound"
  }'
```

### Simulate a transient failure that succeeds on retry

```json
{
  "messageId": "MSG-RETRY-1",
  "transactionId": "TXN-2005",
  "requestId": "REQ-5",
  "accountId": "ACC-203",
  "transactionType": "CREDIT",
  "amount": 500,
  "currency": "USD",
  "sequenceNumber": 1,
  "transientFailuresToSimulate": 1
}
```

### Simulate retry exhaustion
Set `transientFailuresToSimulate` to `4`. With one initial attempt and three retries, the transaction becomes `FAILED` and no balance effect is applied.

### Demonstrate duplicate Kafka delivery
Send the same transaction payload twice with the same `messageId`, but the second request may have a different `requestId` and a different service `receivedAt`. The second observed delivery returns `DUPLICATE`, has its own `recordId` and `receivedAt`, and references the original record using `duplicateOfRecordId`. The balance is changed only once.

Example duplicate request body should retain `"messageId":"MSG-2001"` and the same business fields as the original message.

### Management record lookup
Browse records for a topic and year, one record per page:

```bash
curl "http://localhost:8080/api/v1/management/topics/payments.inbound/records?year=2026&page=0"
```

Inspect all deliveries for a stable message ID, one delivery per page:

```bash
curl "http://localhost:8080/api/v1/management/messages/MSG-2001/deliveries?page=0"
```

Fetch one observed delivery by topic and unique record ID:

```bash
curl "http://localhost:8080/api/v1/management/topics/payments.inbound/records/REPLACE_WITH_RECORD_ID"
```

`page` is zero-based; page size is fixed at one. Results are ordered by `receivedAt` newest-first.

### Endpoints
- `POST /api/transactions`
- `POST /api/transactions/batch`
- `GET /api/transactions`
- `GET /api/transactions/{transactionId}`
- `POST /api/transactions/{transactionId}/retry`
- `GET /api/summary`
- `GET /api/accounts/{accountId}/balance`
- `GET /api/v1/management/topics/{topicName}/records?year=2026&page=0`
- `GET /api/v1/management/topics/{topicName}/records/{recordId}`
- `GET /api/v1/management/messages/{messageId}/deliveries?page=0` (all observed deliveries for one stable message ID, one per page)

## Sample out-of-order scenario
Submit sequence 2 for `ACC-201` before sequence 1. Sequence 2 remains `PENDING`. When sequence 1 arrives, the processor applies sequence 1 and then sequence 2.

## Limitations and production hardening
This assessment version deliberately uses an in-memory repository and synchronized service methods. State is lost on restart, and synchronization does not coordinate multiple application instances. It models Kafka message metadata through `topicName` and `messageId`, but does not yet connect to a running Kafka broker. For production:
- persist audit records, idempotency keys, transaction state, balances and sequence checkpoints in a relational database;
- add unique constraints for `message_id` and business `transaction_id`, and persist duplicate delivery attempts with separate record IDs;
- atomically store the idempotency decision and ledger effect in one database transaction;
- use optimistic/pessimistic locking or per-account partitioning to prevent sequence races;
- use bounded exponential backoff, a durable scheduled replay queue and a dead-letter/recovery workflow;
- use an outbox pattern for reliable downstream Kafka publication;
- add authentication/authorization, rate limiting, metrics, tracing, dashboards and alerting;
- define currency precision, reversal semantics, overdraft policy, sequence gap timeout and reconciliation.

## AI-assisted development note
AI assistance was used to accelerate scaffolding and test-case drafting. Review, build and test the code before submission and be prepared to explain every design choice.

## Configuration (`application.yaml`)

Topic names, replay settings, downstream client URLs/timeouts, and credential placeholders are externalized in `src/main/resources/application.yaml`. Override placeholders using environment variables. Do not commit real passwords, API keys, or tokens.

Examples (PowerShell):

```powershell
$env:KAFKA_TOPIC_PAYMENT_INBOUND = "payments.inbound"
$env:LEDGER_CLIENT_BASE_URL = "https://ledger.example.internal"
$env:LEDGER_CLIENT_USERNAME = "local-dev-user"
$env:LEDGER_CLIENT_PASSWORD = "<provide-through-your-secret-store>"
gradle bootRun
```

These are configuration foundations only: Kafka consumption, the hourly replay scheduler, downstream HTTP clients, and authentication are not connected in this assessment build. The retry settings describe the intended integration contract; the current implementation demonstrates retry processing synchronously through its service.

## Exception and failure scenarios

- **Required field missing / invalid amount, currency, sequence, topic or ID:** the service records the input as `REJECTED`, returns the audit record, and continues processing the rest of a batch.
- **Same `messageId`, same business payload:** this is an expected Kafka redelivery, not an exceptional business failure. A separate delivery/audit record is returned as `DUPLICATE`; no second ledger effect is applied.
- **Same `messageId` or `transactionId`, different business payload:** idempotency-key collision. The new delivery is `REJECTED` and logged for investigation; the original transaction is unchanged.
- **Two transactions claim the same account sequence:** the conflicting record is `REJECTED`; the accepted record remains canonical.
- **Transient processing errors:** retry up to three times after the initial attempt. After four total attempts, state becomes `FAILED`; no ledger side effect is applied for those failed attempts.
- **Insufficient balance / invalid state retry:** transaction remains failed or the retry request is rejected as a conflict; later account sequences are not silently processed out of order.
- **Unknown transaction or management record:** returns HTTP 404.
- **Malformed JSON, invalid query parameter:** returns HTTP 400.
- **Unexpected server error:** global `@RestControllerAdvice` logs details server-side and returns a generic HTTP 500 Problem Details response without leaking stack traces or internal exception messages to the client.

The global exception handler maps validation failures, malformed JSON, duplicate-message exceptions, sequence conflicts, retry-not-allowed, retry-exhausted, invalid-transaction, not-found, and unexpected exceptions to consistent Problem Details responses. Normal duplicate delivery is intentionally represented as a `DUPLICATE` audit result instead of throwing, because idempotent replay is expected in Kafka systems.

## L2 expected deliverables checklist

- **Source code:** `src/main/java` and `src/test/java`.
- **Execution instructions:** this README.
- **Sample input/output:** `sample-transactions.json` and the API examples above; actual response bodies include generated record IDs and timestamps.
- **Unit tests:** `src/test/java/com/mphasis/transactionprocessor/service/TransactionProcessingServiceTest.java` (run with `gradle clean test`).
- **Design note:** `DESIGN.md` covers architecture, idempotency, duplicate detection, out-of-order processing, retries/failures, observability, AI-assisted development, and production hardening.
