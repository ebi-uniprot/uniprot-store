package org.uniprot.store.spark.indexer.common.exception;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
public class IndexDataStoreException extends RuntimeException {

    private static final long serialVersionUID = -3818777367478983644L;

    public IndexDataStoreException(String message) {
        super(message);
    }

    public IndexDataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
