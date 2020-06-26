package org.uniprot.store.spark.indexer.common.exception;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
public class IndexDataStoreException extends RuntimeException {

    public IndexDataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
