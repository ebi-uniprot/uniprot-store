package uk.ac.ebi.uniprot.indexer.common;

import uk.ac.ebi.uniprot.indexer.document.Document;

/**
 * Use this exception when an error occurs whilst converting an entity into an indexable
 * {@link Document}.
 */
public class DocumentConversionException extends IndexingException {
    public DocumentConversionException(String message) {
        super(message);
    }

    public DocumentConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
