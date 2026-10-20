package com.mphasis.transactionprocessor.exception;

public class RetryExhaustedException extends RuntimeException {
    public RetryExhaustedException(String message) { super(message); }
}
