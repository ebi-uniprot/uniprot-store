package uk.ac.ebi.uniprot.indexer;

/**
 * A generic exception which can be thrown during the process of indexing data into one of the UniProt data sources.
 * <p/>
 * Use this exception if the situation that requires it is unrecoverable.
 * <p/>
 * Subclass from this class when the error is recoverable.
 *
 * @author Ricardo Antunes
 */
public class IndexationException extends RuntimeException {
    public IndexationException(String message) {
        super(message);
    }

    public IndexationException(String message, Throwable cause) {
        super(message, cause);
    }
}
