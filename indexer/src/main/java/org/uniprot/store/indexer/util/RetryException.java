package org.uniprot.store.indexer.util;

/**
 * Exception that is thrown when an operation has failed to execute even after several retry
 * attempts.
 *
 * @author Ricardo Antunes
 */
public class RetryException extends RuntimeException {
    public RetryException(String message) {
        super(message);
    }

    public RetryException(String message, Throwable cause) {
        super(message, cause);
    }
}
