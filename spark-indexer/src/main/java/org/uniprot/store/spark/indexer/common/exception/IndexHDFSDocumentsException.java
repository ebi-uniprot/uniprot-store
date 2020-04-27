package org.uniprot.store.spark.indexer.common.exception;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
public class IndexHDFSDocumentsException extends RuntimeException {

    public IndexHDFSDocumentsException(String message, Throwable cause) {
        super(message, cause);
    }
}
