package com.mphasis.transactionprocessor.exception;

/** Raised only by operations whose contract requires a new message. Normal redelivery is recorded as DUPLICATE. */
public class DuplicateMessageException extends RuntimeException {
    public DuplicateMessageException(String message) { super(message); }
}
