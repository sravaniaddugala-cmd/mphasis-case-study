package com.mphasis.transactionprocessor.repository;

import com.mphasis.transactionprocessor.model.TransactionRecord;
import org.springframework.stereotype.Repository;

import java.time.Year;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory repository for the assessment. A production version should enforce the same unique keys in a database.
 * The first record for a messageId/transactionId is the canonical processing record; duplicate deliveries are
 * stored by recordId for audit history but are never allowed to replace the canonical record.
 */
@Repository
public class TransactionRecordRepository {
    private final Map<String, TransactionRecord> byTransactionId = new ConcurrentHashMap<>();
    private final Map<String, TransactionRecord> byMessageId = new ConcurrentHashMap<>();
    private final Map<String, TransactionRecord> byRecordId = new ConcurrentHashMap<>();

    public void save(TransactionRecord record) {
        byTransactionId.putIfAbsent(record.getTransactionId(), record);
        byMessageId.putIfAbsent(record.getMessageId(), record);
        byRecordId.put(record.getRecordId(), record);
    }

    /** Saves an additional observed delivery without replacing canonical transaction/message indexes. */
    public void saveDeliveryOnly(TransactionRecord record) {
        byRecordId.put(record.getRecordId(), record);
    }

    public Optional<TransactionRecord> findByTransactionId(String transactionId) {
        return Optional.ofNullable(byTransactionId.get(transactionId));
    }

    public Optional<TransactionRecord> findByMessageId(String messageId) {
        return Optional.ofNullable(byMessageId.get(messageId));
    }

    /** All observed deliveries with the same stable message ID, newest delivery first. */
    public List<TransactionRecord> findDeliveriesByMessageId(String messageId) {
        return byRecordId.values().stream()
                .filter(record -> record.getMessageId().equals(messageId))
                .sorted(Comparator.comparing(TransactionRecord::getReceivedAt).reversed()
                        .thenComparing(TransactionRecord::getRecordId))
                .toList();
    }

    public Optional<TransactionRecord> findByRecordId(String recordId) {
        return Optional.ofNullable(byRecordId.get(recordId));
    }

    public List<TransactionRecord> findAll() {
        return byRecordId.values().stream().toList();
    }

    public int count() { return byRecordId.size(); }

    public List<TransactionRecord> findByTopicAndYear(String topicName, int year, int page) {
        if (page < 0) throw new IllegalArgumentException("page must be zero or greater");
        Year.of(year);
        long startInclusive = Year.of(year).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long endExclusive = Year.of(year + 1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        List<TransactionRecord> matches = byRecordId.values().stream()
                .filter(record -> record.getTopicName().equals(topicName))
                .filter(record -> {
                    long timestamp = record.getReceivedAt().toEpochMilli();
                    return timestamp >= startInclusive && timestamp < endExclusive;
                })
                .sorted(Comparator.comparing(TransactionRecord::getReceivedAt).reversed()
                        .thenComparing(TransactionRecord::getRecordId))
                .toList();
        return matches.stream().skip(page).limit(1).toList();
    }

    public long countByTopicAndYear(String topicName, int year) {
        Year.of(year);
        long startInclusive = Year.of(year).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        long endExclusive = Year.of(year + 1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
        return byRecordId.values().stream()
                .filter(record -> record.getTopicName().equals(topicName))
                .filter(record -> {
                    long timestamp = record.getReceivedAt().toEpochMilli();
                    return timestamp >= startInclusive && timestamp < endExclusive;
                }).count();
    }
}
