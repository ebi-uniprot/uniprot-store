package org.uniprot.store.reader.publications;

/**
 * Represents a problem with a field.
 *
 * @author Edd
 */
public class RawMappedReferenceException extends RuntimeException {
    public RawMappedReferenceException(String message) {
        super(message);
    }
}
