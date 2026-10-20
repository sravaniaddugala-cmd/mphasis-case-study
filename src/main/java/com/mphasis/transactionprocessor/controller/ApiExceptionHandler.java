package com.mphasis.transactionprocessor.controller;

import com.mphasis.transactionprocessor.exception.DuplicateMessageException;
import com.mphasis.transactionprocessor.exception.InvalidTransactionException;
import com.mphasis.transactionprocessor.exception.RetryExhaustedException;
import com.mphasis.transactionprocessor.exception.RetryNotAllowedException;
import com.mphasis.transactionprocessor.exception.SequenceConflictException;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail validation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        Map<String, String> errors = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            errors.putIfAbsent(error.getField(), error.getDefaultMessage() == null ? "Invalid value" : error.getDefaultMessage());
        }
        ProblemDetail detail = problem(HttpStatus.BAD_REQUEST, "Validation failed",
                "One or more required fields are missing or invalid", request);
        detail.setProperty("fieldErrors", errors);
        return detail;
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ProblemDetail malformedJson(HttpMessageNotReadableException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Malformed request",
                "Request body is not valid JSON or contains an invalid field value", request);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ProblemDetail missingParameter(MissingServletRequestParameterException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Missing request parameter",
                "Required parameter '" + ex.getParameterName() + "' is missing", request);
    }

    @ExceptionHandler(DuplicateMessageException.class)
    public ProblemDetail duplicateMessage(DuplicateMessageException ex, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Duplicate message", ex.getMessage(), request);
    }

    @ExceptionHandler(SequenceConflictException.class)
    public ProblemDetail sequenceConflict(SequenceConflictException ex, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Sequence conflict", ex.getMessage(), request);
    }

    @ExceptionHandler(RetryNotAllowedException.class)
    public ProblemDetail retryNotAllowed(RetryNotAllowedException ex, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Retry not allowed", ex.getMessage(), request);
    }

    @ExceptionHandler(RetryExhaustedException.class)
    public ProblemDetail retryExhausted(RetryExhaustedException ex, HttpServletRequest request) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Retry exhausted", ex.getMessage(), request);
    }

    @ExceptionHandler(InvalidTransactionException.class)
    public ProblemDetail invalidTransaction(InvalidTransactionException ex, HttpServletRequest request) {
        return problem(HttpStatus.UNPROCESSABLE_ENTITY, "Invalid transaction", ex.getMessage(), request);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ProblemDetail notFound(NoSuchElementException ex, HttpServletRequest request) {
        return problem(HttpStatus.NOT_FOUND, "Resource not found", ex.getMessage(), request);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail badRequest(IllegalArgumentException ex, HttpServletRequest request) {
        return problem(HttpStatus.BAD_REQUEST, "Invalid request parameter", ex.getMessage(), request);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail conflict(IllegalStateException ex, HttpServletRequest request) {
        return problem(HttpStatus.CONFLICT, "Invalid state transition", ex.getMessage(), request);
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail unexpected(Exception ex, HttpServletRequest request) {
        log.error("unhandled_api_exception method={} path={} errorType={}",
                request.getMethod(), request.getRequestURI(), ex.getClass().getSimpleName(), ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Internal server error",
                "The request could not be completed. Use the correlation/log ID to investigate.", request);
    }

    private ProblemDetail problem(HttpStatus status, String title, String detail, HttpServletRequest request) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, detail);
        problem.setTitle(title);
        problem.setType(URI.create("https://example.invalid/problems/" + status.value()));
        problem.setInstance(URI.create(request.getRequestURI()));
        problem.setProperty("status", status.value());
        return problem;
    }
}
