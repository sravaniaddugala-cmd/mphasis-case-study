package com.mphasis.transactionprocessor.exception;

public class SequenceConflictException extends RuntimeException {
    public SequenceConflictException(String message) { super(message); }
}
