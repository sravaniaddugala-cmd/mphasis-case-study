package com.mphasis.transactionprocessor.exception;

public class RetryNotAllowedException extends RuntimeException {
    public RetryNotAllowedException(String message) { super(message); }
}
