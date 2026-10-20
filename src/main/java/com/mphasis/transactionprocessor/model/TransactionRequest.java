package com.mphasis.transactionprocessor.model;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;

public class TransactionRequest {
    @NotBlank
    private String transactionId;
    /** Stable producer-generated idempotency key. Reuse this value when republishing the same logical message. */
    private String messageId;
    @NotBlank
    private String requestId;
    @NotBlank
    private String accountId;
    @NotNull
    private TransactionType transactionType;
    @NotNull
    @DecimalMin(value = "0.01", inclusive = true)
    private BigDecimal amount;
    @NotBlank
    private String currency;
    private long sequenceNumber;
    /** Source event timestamp; separate from receivedAt, which is assigned by this service. */
    private Instant eventTime = Instant.now();
    /** Logical source topic for management/audit filtering; defaults when omitted. */
    private String topicName = "transactions.inbound";
    /** Test/demo hook: simulate this many transient failures before processing can succeed. */
    private int transientFailuresToSimulate;
    /** Backwards-compatible demo hook. If true and no count is supplied, simulate one transient failure. */
    private boolean simulateTransientFailure;

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getMessageId() { return messageId; }
    public void setMessageId(String messageId) { this.messageId = messageId; }
    public String getRequestId() { return requestId; }
    public void setRequestId(String requestId) { this.requestId = requestId; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public TransactionType getTransactionType() { return transactionType; }
    public void setTransactionType(TransactionType transactionType) { this.transactionType = transactionType; }
    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }
    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
    public long getSequenceNumber() { return sequenceNumber; }
    public void setSequenceNumber(long sequenceNumber) { this.sequenceNumber = sequenceNumber; }
    public Instant getEventTime() { return eventTime; }
    public void setEventTime(Instant eventTime) { this.eventTime = eventTime; }
    public String getTopicName() { return topicName; }
    public void setTopicName(String topicName) { this.topicName = topicName; }
    public int getTransientFailuresToSimulate() { return transientFailuresToSimulate; }
    public void setTransientFailuresToSimulate(int count) { this.transientFailuresToSimulate = count; }
    public boolean isSimulateTransientFailure() { return simulateTransientFailure; }
    public void setSimulateTransientFailure(boolean simulateTransientFailure) { this.simulateTransientFailure = simulateTransientFailure; }
}
