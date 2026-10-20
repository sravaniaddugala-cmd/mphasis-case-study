package com.mphasis.transactionprocessor.service;

import com.mphasis.transactionprocessor.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.Year;

import static org.junit.jupiter.api.Assertions.*;

class TransactionProcessingServiceTest {
    private TransactionProcessingService service;

    @BeforeEach
    void setUp() {
        service = new TransactionProcessingService();
    }

    @Test
    void duplicateTransactionDoesNotApplyBalanceTwice() {
        TransactionRequest request = request("TX-1", "REQ-1", "ACC-1", 1, "100", TransactionType.CREDIT);
        assertEquals(TransactionStatus.PROCESSED, service.submit(request).getStatus());
        TransactionRecord duplicate = service.submit(request("TX-1", "REQ-1-REPLAY", "ACC-1", 1,
                "100", TransactionType.CREDIT));
        assertEquals(TransactionStatus.DUPLICATE, duplicate.getStatus());
        assertEquals(new BigDecimal("100"), service.balance("ACC-1"));
        assertEquals(1, service.summary().duplicateRequests());
    }

    @Test
    void futureSequenceWaitsUntilMissingSequenceArrives() {
        TransactionRecord second = service.submit(request("TX-2", "REQ-2", "ACC-1", 2, "40", TransactionType.DEBIT));
        assertEquals(TransactionStatus.PENDING, second.getStatus());

        TransactionRecord first = service.submit(request("TX-1", "REQ-1", "ACC-1", 1, "100", TransactionType.CREDIT));
        assertEquals(TransactionStatus.PROCESSED, first.getStatus());
        assertEquals(TransactionStatus.PROCESSED, service.findById("TX-2").orElseThrow().getStatus());
        assertEquals(new BigDecimal("60"), service.balance("ACC-1"));
    }

    @Test
    void invalidAmountIsRejectedWithoutStoppingProcessing() {
        TransactionRequest invalid = request("TX-BAD", "REQ-BAD", "ACC-1", 1, "-10", TransactionType.DEBIT);
        assertEquals(TransactionStatus.REJECTED, service.submit(invalid).getStatus());
        assertEquals(BigDecimal.ZERO, service.balance("ACC-1"));
        assertEquals(0, service.summary().processedTransactions());
    }

    @Test
    void invalidBatchRecordDoesNotStopFollowingRecords() {
        TransactionRequest invalid = request("TX-BATCH-BAD", "REQ-BATCH-BAD", "ACC-BATCH-BAD", 1,
                "-1", TransactionType.CREDIT);
        TransactionRequest valid = request("TX-BATCH-GOOD", "REQ-BATCH-GOOD", "ACC-BATCH-GOOD", 1,
                "20", TransactionType.CREDIT);

        var results = service.submitBatch(java.util.List.of(invalid, valid));
        assertEquals(2, results.size());
        assertEquals(TransactionStatus.REJECTED, results.get(0).getStatus());
        assertEquals(TransactionStatus.PROCESSED, results.get(1).getStatus());
        assertEquals(new BigDecimal("20"), service.balance("ACC-BATCH-GOOD"));
    }

    @Test
    void stateMachineRejectsInvalidTransition() {
        TransactionRecord record = new TransactionRecord(
                request("TX-STATE", "REQ-STATE", "ACC-STATE", 1, "10", TransactionType.CREDIT));
        assertThrows(IllegalStateException.class, () -> record.setStatus(TransactionStatus.PROCESSED));
    }

    @Test
    void transientFailureRetriesWithoutDuplicateEffect() {
        TransactionRequest retryRequest = request("TX-RETRY", "REQ-RETRY", "ACC-1", 1,
                "500", TransactionType.CREDIT);
        retryRequest.setMessageId("MSG-RETRY-1");
        retryRequest.setTransientFailuresToSimulate(1);
        TransactionRecord result = service.submit(retryRequest);
        assertEquals(TransactionStatus.PROCESSED, result.getStatus());
        assertEquals(new BigDecimal("500"), service.balance("ACC-1"));
        assertEquals(2, result.getAttempts());
    }

    @Test
    void managementCanFilterByTopicAndYearAndFetchExactRecord() {
        TransactionRequest request = request("TX-MGMT-1", "REQ-MGMT-1", "ACC-MGMT", 1,
                "25", TransactionType.CREDIT);
        request.setTopicName("payments.inbound");
        request.setEventTime(Instant.now());

        TransactionRecord saved = service.submit(request);
        assertNotNull(saved.getRecordId());
        assertEquals("payments.inbound", saved.getTopicName());

        SingleRecordPage page = service.findManagementRecords("payments.inbound", Year.now().getValue(), 0);
        assertEquals(1, page.pageSize());
        assertEquals(1, page.totalRecords());
        assertEquals(saved.getRecordId(), page.record().getRecordId());
        assertEquals(saved.getRecordId(), service.findManagementRecord(
                "payments.inbound", saved.getRecordId()).orElseThrow().getRecordId());
        assertTrue(service.findManagementRecord("invoices.inbound", saved.getRecordId()).isEmpty());
    }

    @Test
    void conflictingSequenceIsRejected() {
        service.submit(request("TX-1", "REQ-1", "ACC-1", 1, "100", TransactionType.CREDIT));
        TransactionRecord conflict = service.submit(request("TX-OTHER", "REQ-OTHER", "ACC-1", 1,
                "50", TransactionType.CREDIT));
        assertEquals(TransactionStatus.REJECTED, conflict.getStatus());
        assertEquals(new BigDecimal("100"), service.balance("ACC-1"));
    }

    @Test
    void sameMessageIdWithDifferentDeliveryTimestampDoesNotDuplicateSideEffect() {
        TransactionRequest first = request("TX-MSG-1", "REQ-1", "ACC-MSG", 1, "75", TransactionType.CREDIT);
        first.setMessageId("KAFKA-MSG-100");
        TransactionRecord processed = service.submit(first);
        assertEquals(TransactionStatus.PROCESSED, processed.getStatus());

        TransactionRequest redelivery = request("TX-MSG-1", "REQ-REDELIVERY", "ACC-MSG", 1,
                "75", TransactionType.CREDIT);
        redelivery.setMessageId("KAFKA-MSG-100");
        TransactionRecord duplicate = service.submit(redelivery);

        assertEquals(TransactionStatus.DUPLICATE, duplicate.getStatus());
        assertEquals("KAFKA-MSG-100", duplicate.getMessageId());
        assertNotNull(processed.getReceivedAt());
        assertNotNull(duplicate.getReceivedAt());
        assertEquals(processed.getRecordId(), duplicate.getDuplicateOfRecordId());
        assertEquals(2, service.findDeliveriesByMessageId("KAFKA-MSG-100", 0).totalRecords());
        assertEquals(duplicate.getRecordId(), service.findDeliveriesByMessageId("KAFKA-MSG-100", 0).record().getRecordId());
        assertEquals(new BigDecimal("75"), service.balance("ACC-MSG"));
    }

    @Test
    void reusedMessageIdWithDifferentPayloadIsRejected() {
        TransactionRequest first = request("TX-COLLISION-1", "REQ-1", "ACC-COLLISION", 1,
                "50", TransactionType.CREDIT);
        first.setMessageId("KAFKA-MSG-COLLISION");
        assertEquals(TransactionStatus.PROCESSED, service.submit(first).getStatus());

        TransactionRequest altered = request("TX-COLLISION-2", "REQ-2", "ACC-COLLISION", 2,
                "500", TransactionType.CREDIT);
        altered.setMessageId("KAFKA-MSG-COLLISION");
        assertEquals(TransactionStatus.REJECTED, service.submit(altered).getStatus());
        assertEquals(new BigDecimal("50"), service.balance("ACC-COLLISION"));
    }

    @Test
    void retryExhaustionDoesNotApplyAnyBalanceEffect() {
        TransactionRequest request = request("TX-EXHAUST", "REQ-EXHAUST", "ACC-EXHAUST", 1,
                "100", TransactionType.CREDIT);
        request.setMessageId("MSG-EXHAUST");
        request.setTransientFailuresToSimulate(4); // initial attempt + three retries all fail
        TransactionRecord result = service.submit(request);
        assertEquals(TransactionStatus.FAILED, result.getStatus());
        assertEquals(4, result.getAttempts());
        assertEquals(BigDecimal.ZERO, service.balance("ACC-EXHAUST"));
        assertTrue(result.getMessage().contains("Retry exhaustion"));
    }

    private TransactionRequest request(String id, String requestId, String account, long seq,
                                       String amount, TransactionType type) {
        TransactionRequest r = new TransactionRequest();
        r.setTransactionId(id);
        r.setRequestId(requestId);
        r.setAccountId(account);
        r.setSequenceNumber(seq);
        r.setAmount(new BigDecimal(amount));
        r.setCurrency("USD");
        r.setTransactionType(type);
        return r;
    }
}
