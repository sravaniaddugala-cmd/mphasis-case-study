package com.mphasis.transactionprocessor.model;

/** One-delivery-per-page view for all observed deliveries sharing the same stable messageId. */
public record MessageDeliveryPage(
        String messageId,
        int page,
        int pageSize,
        long totalRecords,
        long totalPages,
        TransactionRecord record) {
}
