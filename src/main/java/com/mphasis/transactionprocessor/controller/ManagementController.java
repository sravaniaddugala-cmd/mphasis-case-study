package com.mphasis.transactionprocessor.controller;

import com.mphasis.transactionprocessor.model.SingleRecordPage;
import com.mphasis.transactionprocessor.model.MessageDeliveryPage;
import com.mphasis.transactionprocessor.model.TransactionRecord;
import com.mphasis.transactionprocessor.service.TransactionProcessingService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Year;
import java.util.NoSuchElementException;

/** Read-only management endpoints. Each search page intentionally returns at most one record. */
@RestController
@RequestMapping("/api/v1/management")
public class ManagementController {
    private final TransactionProcessingService service;

    public ManagementController(TransactionProcessingService service) {
        this.service = service;
    }

    /** Browse a topic's records for a given calendar year, one record per page. */
    @GetMapping("/topics/{topicName}/records")
    public SingleRecordPage recordsByTopicAndYear(
            @PathVariable String topicName,
            @RequestParam int year,
            @RequestParam(defaultValue = "0") int page) {
        if (year < 1970 || year > Year.now().getValue() + 1) {
            throw new IllegalArgumentException("year must be between 1970 and next calendar year");
        }
        if (page < 0) {
            throw new IllegalArgumentException("page must be zero or greater");
        }
        return service.findManagementRecords(topicName, year, page);
    }

    /** Inspect every observed delivery for a stable message ID, one delivery per page. */
    @GetMapping("/messages/{messageId}/deliveries")
    public MessageDeliveryPage deliveriesByMessageId(
            @PathVariable String messageId,
            @RequestParam(defaultValue = "0") int page) {
        return service.findDeliveriesByMessageId(messageId, page);
    }

    /** Fetch one exact record using its unique recordId within the specified topic. */
    @GetMapping("/topics/{topicName}/records/{recordId}")
    public TransactionRecord recordByTopicAndId(
            @PathVariable String topicName,
            @PathVariable String recordId) {
        return service.findManagementRecord(topicName, recordId)
                .orElseThrow(() -> new NoSuchElementException(
                        "No record found for topic '" + topicName + "' and recordId '" + recordId + "'"));
    }
}
