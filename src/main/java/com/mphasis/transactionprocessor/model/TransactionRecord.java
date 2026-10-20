package com.mphasis.transactionprocessor.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;

public class TransactionRecord {
    private static final Map<TransactionStatus, EnumMap<TransactionStatus, Boolean>> ALLOWED = allowedTransitions();

    /** Unique identifier for this observed delivery/audit record. */
    private final String recordId;
    /** Stable logical message identifier supplied by the producer and reused on redelivery. */
    private final String messageId;
    private final String topicName;
    private final String transactionId;
    private final String requestId;
    private final String accountId;
    private final TransactionType transactionType;
    private final BigDecimal amount;
    private final String currency;
    private final long sequenceNumber;
    private final Instant eventTime;
    /** Timestamp for this observed delivery; can differ across duplicate deliveries. */
    private final Instant receivedAt;
    private TransactionStatus status;
    private int attempts;
    private int retriesInCurrentCycle;
    private int simulatedTransientFailuresRemaining;
    private String message;
    private String duplicateOfRecordId;

    public TransactionRecord(TransactionRequest request) {
        this.recordId = UUID.randomUUID().toString();
        this.messageId = request.getMessageId() == null || request.getMessageId().isBlank()
                ? request.getRequestId().trim() : request.getMessageId().trim();
        this.topicName = request.getTopicName() == null || request.getTopicName().isBlank()
                ? "transactions.inbound" : request.getTopicName().trim();
        this.transactionId = request.getTransactionId();
        this.requestId = request.getRequestId();
        this.accountId = request.getAccountId();
        this.transactionType = request.getTransactionType();
        this.amount = request.getAmount();
        this.currency = request.getCurrency();
        this.sequenceNumber = request.getSequenceNumber();
        this.eventTime = request.getEventTime() == null ? Instant.now() : request.getEventTime();
        this.receivedAt = Instant.now();
        this.status = TransactionStatus.RECEIVED;
        this.message = "Accepted for processing";
        this.simulatedTransientFailuresRemaining = Math.max(0, request.getTransientFailuresToSimulate());
        if (request.isSimulateTransientFailure() && this.simulatedTransientFailuresRemaining == 0) {
            this.simulatedTransientFailuresRemaining = 1;
        }
    }

    public String getRecordId() { return recordId; }
    public String getMessageId() { return messageId; }
    public String getTopicName() { return topicName; }
    public String getTransactionId() { return transactionId; }
    public String getRequestId() { return requestId; }
    public String getAccountId() { return accountId; }
    public TransactionType getTransactionType() { return transactionType; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public long getSequenceNumber() { return sequenceNumber; }
    public Instant getEventTime() { return eventTime; }
    public Instant getReceivedAt() { return receivedAt; }
    public TransactionStatus getStatus() { return status; }

    /** Enforces an explicit state-transition model instead of allowing arbitrary status changes. */
    public void setStatus(TransactionStatus next) {
        if (next == status) return;
        if (!ALLOWED.getOrDefault(status, new EnumMap<>(TransactionStatus.class)).containsKey(next)) {
            throw new IllegalStateException("Invalid transaction state transition: " + status + " -> " + next);
        }
        status = next;
    }

    public int getAttempts() { return attempts; }
    public void incrementAttempts() { attempts++; }
    public int getRetriesInCurrentCycle() { return retriesInCurrentCycle; }
    public void incrementRetriesInCurrentCycle() { retriesInCurrentCycle++; }
    public void resetRetriesInCurrentCycle() { retriesInCurrentCycle = 0; }
    public boolean consumeSimulatedTransientFailure() {
        if (simulatedTransientFailuresRemaining <= 0) return false;
        simulatedTransientFailuresRemaining--;
        return true;
    }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getDuplicateOfRecordId() { return duplicateOfRecordId; }
    public void setDuplicateOfRecordId(String duplicateOfRecordId) { this.duplicateOfRecordId = duplicateOfRecordId; }

    private static Map<TransactionStatus, EnumMap<TransactionStatus, Boolean>> allowedTransitions() {
        Map<TransactionStatus, EnumMap<TransactionStatus, Boolean>> map = new EnumMap<>(TransactionStatus.class);
        allow(map, TransactionStatus.RECEIVED, TransactionStatus.PROCESSING, TransactionStatus.PENDING,
                TransactionStatus.RETRY_PENDING, TransactionStatus.FAILED, TransactionStatus.REJECTED,
                TransactionStatus.DUPLICATE);
        allow(map, TransactionStatus.PENDING, TransactionStatus.RECEIVED, TransactionStatus.PROCESSING,
                TransactionStatus.REJECTED, TransactionStatus.FAILED);
        allow(map, TransactionStatus.PROCESSING, TransactionStatus.PROCESSED, TransactionStatus.RETRY_PENDING,
                TransactionStatus.FAILED, TransactionStatus.REVIEW_REQUIRED);
        allow(map, TransactionStatus.RETRY_PENDING, TransactionStatus.PROCESSING, TransactionStatus.FAILED,
                TransactionStatus.REVIEW_REQUIRED, TransactionStatus.RECEIVED);
        allow(map, TransactionStatus.FAILED, TransactionStatus.RECEIVED, TransactionStatus.RETRY_PENDING,
                TransactionStatus.REVIEW_REQUIRED);
        allow(map, TransactionStatus.REVIEW_REQUIRED, TransactionStatus.RECEIVED, TransactionStatus.PROCESSING);
        return map;
    }

    private static void allow(Map<TransactionStatus, EnumMap<TransactionStatus, Boolean>> map,
                              TransactionStatus from, TransactionStatus... toStates) {
        EnumMap<TransactionStatus, Boolean> targets = map.computeIfAbsent(from,
                ignored -> new EnumMap<>(TransactionStatus.class));
        for (TransactionStatus to : toStates) targets.put(to, Boolean.TRUE);
    }
}
