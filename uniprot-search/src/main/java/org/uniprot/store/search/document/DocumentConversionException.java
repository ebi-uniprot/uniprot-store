package org.uniprot.store.search.document;

/**
 * Use this exception when an error occurs whilst converting an entity into an indexable documents.
 */
public class DocumentConversionException extends RuntimeException {

    private static final long serialVersionUID = 6908314271937596010L;

    public DocumentConversionException(String message) {
        super(message);
    }

    public DocumentConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
