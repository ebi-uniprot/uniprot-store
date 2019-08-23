package org.uniprot.store.job.common;

/**
 * A generic exception which can be thrown during the process of storing data into one of the UniProt data sources.
 * <p/>
 * Use this exception if the situation that requires it is unrecoverable.
 * <p/>
 * Subclass from this class when the error is recoverable.
 *
 * @author Ricardo Antunes
 */
public class StoringException extends RuntimeException {
    public StoringException(String message) {
        super(message);
    }

    public StoringException(String message, Throwable cause) {
        super(message, cause);
    }
}
