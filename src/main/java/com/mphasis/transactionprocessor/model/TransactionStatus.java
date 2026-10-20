package com.mphasis.transactionprocessor.model;

/** Public lifecycle states for transaction processing and audit visibility. */
public enum TransactionStatus {
    RECEIVED,
    PROCESSING,
    PENDING,
    RETRY_PENDING,
    PROCESSED,
    DUPLICATE,
    REJECTED,
    FAILED,
    REVIEW_REQUIRED
}
