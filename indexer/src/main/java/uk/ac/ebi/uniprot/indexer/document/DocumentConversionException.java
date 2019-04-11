package uk.ac.ebi.uniprot.indexer.document;

import uk.ac.ebi.uniprot.indexer.IndexationException;
import uk.ac.ebi.uniprot.search.document.uk;

/**
 * Use this exception when an error occurs whilst converting an entry into an indexable {@link
 * uk.ac.ebi.uniprot.dataservice.document.Document}.
 */
public class DocumentConversionException extends IndexationException {
    public DocumentConversionException(String message) {
        super(message);
    }

    public DocumentConversionException(String message, Throwable cause) {
        super(message, cause);
    }
}
