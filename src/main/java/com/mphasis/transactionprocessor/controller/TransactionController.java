package com.mphasis.transactionprocessor.controller;

import com.mphasis.transactionprocessor.model.*;
import com.mphasis.transactionprocessor.service.TransactionProcessingService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api")
public class TransactionController {
    private final TransactionProcessingService service;

    public TransactionController(TransactionProcessingService service) {
        this.service = service;
    }

    @PostMapping("/transactions")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public TransactionRecord submit(@RequestBody TransactionRequest request) {
        return service.submit(request);
    }

    @PostMapping("/transactions/batch")
    public List<TransactionRecord> submitBatch(@RequestBody List<TransactionRequest> requests) {
        return service.submitBatch(requests);
    }

    @GetMapping("/transactions")
    public List<TransactionRecord> all() {
        return service.findAll();
    }

    @GetMapping("/transactions/{transactionId}")
    public TransactionRecord byId(@PathVariable String transactionId) {
        return service.findById(transactionId)
                .orElseThrow(() -> new NoSuchElementException("Transaction not found: " + transactionId));
    }

    @PostMapping("/transactions/{transactionId}/retry")
    public TransactionRecord retry(@PathVariable String transactionId) {
        return service.retry(transactionId);
    }

    @GetMapping("/summary")
    public ProcessingSummary summary() {
        return service.summary();
    }

    @GetMapping("/accounts/{accountId}/balance")
    public Map<String, Object> balance(@PathVariable String accountId) {
        BigDecimal balance = service.balance(accountId);
        return Map.of("accountId", accountId, "balance", balance);
    }
}
