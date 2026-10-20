package com.mphasis.transactionprocessor.model;

import java.util.Map;

public record ProcessingSummary(long total, Map<TransactionStatus, Long> counts,
                                long duplicateRequests, long processedTransactions) {
}
