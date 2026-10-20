package com.mphasis.transactionprocessor.model;

public record SingleRecordPage(
        String topicName,
        int year,
        int page,
        int pageSize,
        long totalRecords,
        long totalPages,
        TransactionRecord record) {
}
