package org.uniprot.store.job.common;

/**
 * Use this exception when an error occurs whilst converting an entity into an indexable documents.
 */
public class DocumentConversionException extends StoringException {
    public DocumentConversionException(String message) {
        super(message);
    }

    public DocumentConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
