package com.mphasis.transactionprocessor.service;

import com.mphasis.transactionprocessor.model.*;
import com.mphasis.transactionprocessor.repository.TransactionRecordRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class TransactionProcessingService {
    private static final Logger log = LoggerFactory.getLogger(TransactionProcessingService.class);
    /** Three retries after the initial attempt: at most four attempts in one retry cycle. */
    private static final int MAX_RETRIES = 3;

    // In-memory stores keep this assessment self-contained. Replace with database-backed stores in production.
    private final TransactionRecordRepository recordRepository;
    private final Map<String, TransactionRecord> sequenceIndex = new ConcurrentHashMap<>();
    private final Map<String, BigDecimal> balances = new ConcurrentHashMap<>();
    private final Map<String, Long> nextSequenceByAccount = new ConcurrentHashMap<>();
    private long duplicateRequests;

    @Autowired
    public TransactionProcessingService(TransactionRecordRepository recordRepository) {
        this.recordRepository = recordRepository;
    }

    public TransactionProcessingService() { this(new TransactionRecordRepository()); }

    public synchronized TransactionRecord submit(TransactionRequest request) {
        String validationError = validate(request);
        if (validationError != null) {
            TransactionRecord invalid = safeRecord(request);
            invalid.setStatus(TransactionStatus.REJECTED);
            invalid.setMessage(validationError);
            recordRepository.saveDeliveryOnly(invalid);
            log.warn("transaction_rejected messageId={} transactionId={} reason={}",
                    invalid.getMessageId(), invalid.getTransactionId(), validationError);
            return invalid;
        }

        // The messageId must be stable across producer redelivery. receivedAt is deliberately separate.
        TransactionRecord byMessageId = recordRepository.findByMessageId(effectiveMessageId(request)).orElse(null);
        if (byMessageId != null) {
            return recordDuplicateDelivery(request, byMessageId, "Message ID has already been observed");
        }

        TransactionRecord byTransactionId = recordRepository.findByTransactionId(request.getTransactionId()).orElse(null);
        if (byTransactionId != null) {
            return recordDuplicateDelivery(request, byTransactionId,
                    "Transaction ID has already been accepted; no additional effect applied");
        }

        String sequenceKey = sequenceKey(request.getAccountId(), request.getSequenceNumber());
        if (sequenceIndex.containsKey(sequenceKey)) {
            TransactionRecord conflict = new TransactionRecord(request);
            conflict.setStatus(TransactionStatus.REJECTED);
            conflict.setMessage("Another transaction already uses this account sequence number");
            recordRepository.save(conflict);
            log.warn("transaction_rejected messageId={} transactionId={} reason=sequence_conflict",
                    conflict.getMessageId(), conflict.getTransactionId());
            return conflict;
        }

        TransactionRecord record = new TransactionRecord(request);
        recordRepository.save(record);
        sequenceIndex.put(sequenceKey, record);
        nextSequenceByAccount.putIfAbsent(record.getAccountId(), 1L);
        log.info("transaction_received messageId={} recordId={} transactionId={} accountId={} sequence={} receivedAt={}",
                record.getMessageId(), record.getRecordId(), record.getTransactionId(), record.getAccountId(),
                record.getSequenceNumber(), record.getReceivedAt());

        processAvailable(record.getAccountId());
        return recordRepository.findByTransactionId(record.getTransactionId()).orElse(record);
    }

    private TransactionRecord recordDuplicateDelivery(TransactionRequest request, TransactionRecord original,
                                                        String duplicateMessage) {
        TransactionRecord duplicate = new TransactionRecord(request);
        duplicate.setDuplicateOfRecordId(original.getRecordId());
        if (!sameBusinessPayload(request, original)) {
            duplicate.setStatus(TransactionStatus.REJECTED);
            duplicate.setMessage("Idempotency key collision: messageId/transactionId was reused with a different payload");
            log.error("idempotency_key_collision messageId={} transactionId={} originalRecordId={}",
                    duplicate.getMessageId(), duplicate.getTransactionId(), original.getRecordId());
        } else {
            duplicate.setStatus(TransactionStatus.DUPLICATE);
            duplicate.setMessage(duplicateMessage + "; no additional side effect applied");
            duplicateRequests++;
            log.info("duplicate_detected messageId={} transactionId={} duplicateRecordId={} originalRecordId={} receivedAt={}",
                    duplicate.getMessageId(), duplicate.getTransactionId(), duplicate.getRecordId(),
                    original.getRecordId(), duplicate.getReceivedAt());
        }
        // Preserve each delivery timestamp and recordId for management/audit, without replacing canonical indexes.
        recordRepository.saveDeliveryOnly(duplicate);
        return duplicate;
    }

    private boolean sameBusinessPayload(TransactionRequest request, TransactionRecord original) {
        return Objects.equals(request.getTransactionId(), original.getTransactionId())
                && Objects.equals(request.getAccountId(), original.getAccountId())
                && request.getTransactionType() == original.getTransactionType()
                && request.getAmount() != null && request.getAmount().compareTo(original.getAmount()) == 0
                && Objects.equals(request.getCurrency(), original.getCurrency())
                && request.getSequenceNumber() == original.getSequenceNumber();
    }

    private String effectiveMessageId(TransactionRequest request) {
        return request.getMessageId() == null || request.getMessageId().isBlank()
                ? request.getRequestId().trim() : request.getMessageId().trim();
    }

    public synchronized List<TransactionRecord> submitBatch(List<TransactionRequest> requests) {
        if (requests == null) return List.of();
        List<TransactionRecord> results = new ArrayList<>();
        // Each record is isolated: a bad item produces its own REJECTED result and does not stop the batch.
        for (TransactionRequest request : requests) {
            try {
                results.add(submit(request));
            } catch (RuntimeException ex) {
                log.error("batch_record_unexpected_error transactionId={} error={}",
                        request == null ? null : request.getTransactionId(), ex.getMessage(), ex);
                TransactionRecord rejected = safeRecord(request);
                if (rejected.getStatus() != TransactionStatus.REJECTED) {
                    rejected.setStatus(TransactionStatus.REJECTED);
                }
                rejected.setMessage("Unexpected processing error: " + ex.getMessage());
                recordRepository.saveDeliveryOnly(rejected);
                results.add(rejected);
            }
        }
        return results;
    }

    private void processAvailable(String accountId) {
        long expected = nextSequenceByAccount.getOrDefault(accountId, 1L);
        while (true) {
            TransactionRecord record = sequenceIndex.get(sequenceKey(accountId, expected));
            if (record == null) {
                markLaterRecordsPending(accountId, expected);
                return;
            }
            if (record.getStatus() == TransactionStatus.PROCESSED) {
                expected++;
                nextSequenceByAccount.put(accountId, expected);
                continue;
            }
            if (record.getStatus() == TransactionStatus.FAILED || record.getStatus() == TransactionStatus.REVIEW_REQUIRED) {
                return; // Do not skip a failed sequence and silently change business order.
            }
            if (record.getStatus() == TransactionStatus.PENDING) {
                record.setStatus(TransactionStatus.RECEIVED);
            }
            if (record.getStatus() != TransactionStatus.RECEIVED
                    && record.getStatus() != TransactionStatus.RETRY_PENDING) return;

            boolean completed = processOne(record);
            if (!completed) return;
            expected++;
            nextSequenceByAccount.put(accountId, expected);
        }
    }

    private boolean processOne(TransactionRecord record) {
        for (int retryNumber = 0; retryNumber <= MAX_RETRIES; retryNumber++) {
            if (record.getStatus() != TransactionStatus.PROCESSING) {
                record.setStatus(TransactionStatus.PROCESSING);
            }
            record.incrementAttempts();

            // Test hook: simulate transient errors before the business side effect is applied.
            if (record.consumeSimulatedTransientFailure()) {
                record.setMessage("Simulated transient failure on attempt " + record.getAttempts());
                if (retryNumber < MAX_RETRIES) {
                    record.setStatus(TransactionStatus.RETRY_PENDING);
                    record.incrementRetriesInCurrentCycle();
                    log.warn("transaction_retry messageId={} transactionId={} attempt={} retry={}/{}",
                            record.getMessageId(), record.getTransactionId(), record.getAttempts(),
                            record.getRetriesInCurrentCycle(), MAX_RETRIES);
                    continue;
                }
                record.setStatus(TransactionStatus.FAILED);
                record.setMessage("Retry exhaustion: maximum of " + MAX_RETRIES + " retries reached");
                log.error("transaction_retry_exhausted messageId={} transactionId={} attempts={}",
                        record.getMessageId(), record.getTransactionId(), record.getAttempts());
                return false;
            }

            BigDecimal oldBalance = balances.getOrDefault(record.getAccountId(), BigDecimal.ZERO);
            BigDecimal newBalance;
            switch (record.getTransactionType()) {
                case CREDIT, REVERSAL -> newBalance = oldBalance.add(record.getAmount());
                case DEBIT -> {
                    if (oldBalance.compareTo(record.getAmount()) < 0) {
                        record.setStatus(TransactionStatus.FAILED);
                        record.setMessage("Insufficient balance; transaction was not posted");
                        log.warn("transaction_failed messageId={} transactionId={} reason=insufficient_balance",
                                record.getMessageId(), record.getTransactionId());
                        return false;
                    }
                    newBalance = oldBalance.subtract(record.getAmount());
                }
                default -> {
                    record.setStatus(TransactionStatus.REJECTED);
                    record.setMessage("Unsupported transaction type");
                    return false;
                }
            }

            // The side effect occurs exactly once, after transient failure checks and business validation.
            balances.put(record.getAccountId(), newBalance);
            record.setStatus(TransactionStatus.PROCESSED);
            record.setMessage("Processed successfully");
            recordRepository.save(record);
            log.info("transaction_processed messageId={} recordId={} transactionId={} accountId={} balance={} attempts={}",
                    record.getMessageId(), record.getRecordId(), record.getTransactionId(),
                    record.getAccountId(), newBalance, record.getAttempts());
            return true;
        }
        return false;
    }

    private void markLaterRecordsPending(String accountId, long expected) {
        sequenceIndex.values().stream()
                .filter(r -> r.getAccountId().equals(accountId)
                        && r.getSequenceNumber() > expected
                        && r.getStatus() == TransactionStatus.RECEIVED)
                .forEach(r -> {
                    r.setStatus(TransactionStatus.PENDING);
                    r.setMessage("Waiting for sequence " + expected);
                });
    }

    public synchronized Optional<TransactionRecord> findById(String transactionId) {
        return recordRepository.findByTransactionId(transactionId);
    }

    public synchronized List<TransactionRecord> findAll() {
        return recordRepository.findAll().stream()
                .sorted(Comparator.comparing(TransactionRecord::getAccountId)
                        .thenComparingLong(TransactionRecord::getSequenceNumber)
                        .thenComparing(TransactionRecord::getReceivedAt))
                .collect(Collectors.toList());
    }

    public synchronized ProcessingSummary summary() {
        Map<TransactionStatus, Long> counts = new EnumMap<>(TransactionStatus.class);
        for (TransactionStatus status : TransactionStatus.values()) counts.put(status, 0L);
        recordRepository.findAll().forEach(r -> counts.merge(r.getStatus(), 1L, Long::sum));
        return new ProcessingSummary(recordRepository.count(), counts, duplicateRequests,
                counts.get(TransactionStatus.PROCESSED));
    }

    public synchronized MessageDeliveryPage findDeliveriesByMessageId(String messageId, int page) {
        if (messageId == null || messageId.isBlank()) throw new IllegalArgumentException("messageId is required");
        if (page < 0) throw new IllegalArgumentException("page must be zero or greater");
        List<TransactionRecord> deliveries = recordRepository.findDeliveriesByMessageId(messageId);
        return new MessageDeliveryPage(messageId, page, 1, deliveries.size(), deliveries.size(),
                deliveries.stream().skip(page).findFirst().orElse(null));
    }

    public synchronized Optional<TransactionRecord> findManagementRecord(String topicName, String recordId) {
        return recordRepository.findByRecordId(recordId).filter(record -> record.getTopicName().equals(topicName));
    }

    public synchronized SingleRecordPage findManagementRecords(String topicName, int year, int page) {
        if (topicName == null || topicName.isBlank()) throw new IllegalArgumentException("topicName is required");
        List<TransactionRecord> pageRecords = recordRepository.findByTopicAndYear(topicName, year, page);
        long total = recordRepository.countByTopicAndYear(topicName, year);
        return new SingleRecordPage(topicName, year, page, 1, total, total,
                pageRecords.isEmpty() ? null : pageRecords.get(0));
    }

    public synchronized BigDecimal balance(String accountId) {
        return balances.getOrDefault(accountId, BigDecimal.ZERO);
    }

    public synchronized TransactionRecord retry(String transactionId) {
        TransactionRecord record = recordRepository.findByTransactionId(transactionId)
                .orElseThrow(() -> new NoSuchElementException("Transaction not found: " + transactionId));
        if (record.getStatus() != TransactionStatus.FAILED) {
            throw new IllegalStateException("Only FAILED transactions can be manually retried");
        }
        record.resetRetriesInCurrentCycle();
        record.setStatus(TransactionStatus.RECEIVED);
        record.setMessage("Manual retry requested; new retry cycle started");
        processAvailable(record.getAccountId());
        return record;
    }

    private String validate(TransactionRequest r) {
        if (r == null) return "Request body is required";
        if (blank(r.getTransactionId())) return "transactionId is required";
        if (blank(r.getRequestId())) return "requestId is required";
        if (r.getMessageId() != null && !r.getMessageId().isBlank()
                && !r.getMessageId().matches("[A-Za-z0-9._:-]{1,200}"))
            return "messageId may contain only letters, digits, dot, underscore, colon, and hyphen";
        if (blank(r.getAccountId())) return "accountId is required";
        if (r.getTransactionType() == null) return "transactionType is required";
        if (r.getAmount() == null || r.getAmount().compareTo(BigDecimal.ZERO) <= 0)
            return "amount must be greater than zero";
        if (r.getTopicName() != null && !r.getTopicName().isBlank()
                && !r.getTopicName().matches("[A-Za-z0-9._-]{1,249}"))
            return "topicName may contain only letters, digits, dot, underscore, and hyphen";
        if (blank(r.getCurrency()) || !r.getCurrency().matches("[A-Z]{3}"))
            return "currency must be a 3-letter uppercase code";
        if (r.getSequenceNumber() <= 0) return "sequenceNumber must be greater than zero";
        if (r.getTransientFailuresToSimulate() < 0) return "transientFailuresToSimulate cannot be negative";
        return null;
    }

    private boolean blank(String value) { return value == null || value.isBlank(); }

    private TransactionRecord safeRecord(TransactionRequest request) {
        TransactionRequest safe = request;
        if (safe == null) {
            safe = new TransactionRequest();
            safe.setTransactionId("INVALID-" + UUID.randomUUID());
            safe.setRequestId("INVALID");
            safe.setAccountId("INVALID");
            safe.setTransactionType(TransactionType.CREDIT);
            safe.setAmount(BigDecimal.ONE);
            safe.setCurrency("USD");
            safe.setSequenceNumber(1);
        }
        if (safe.getAmount() == null || safe.getTransactionType() == null
                || safe.getTransactionId() == null || safe.getRequestId() == null
                || safe.getAccountId() == null || safe.getCurrency() == null) {
            TransactionRequest normalized = new TransactionRequest();
            normalized.setTransactionId(safe.getTransactionId() == null ? "INVALID-" + UUID.randomUUID() : safe.getTransactionId());
            normalized.setMessageId(safe.getMessageId());
            normalized.setRequestId(safe.getRequestId() == null ? "INVALID" : safe.getRequestId());
            normalized.setAccountId(safe.getAccountId() == null ? "INVALID" : safe.getAccountId());
            normalized.setTransactionType(safe.getTransactionType() == null ? TransactionType.CREDIT : safe.getTransactionType());
            normalized.setAmount(safe.getAmount() == null ? BigDecimal.ONE : safe.getAmount());
            normalized.setCurrency(safe.getCurrency() == null ? "USD" : safe.getCurrency());
            normalized.setSequenceNumber(Math.max(1, safe.getSequenceNumber()));
            normalized.setTopicName(safe.getTopicName());
            normalized.setEventTime(safe.getEventTime());
            safe = normalized;
        }
        return new TransactionRecord(safe);
    }

    private String sequenceKey(String accountId, long sequence) { return accountId + "::" + sequence; }
}
