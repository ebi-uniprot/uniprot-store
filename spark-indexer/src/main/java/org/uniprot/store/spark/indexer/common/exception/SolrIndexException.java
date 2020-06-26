package org.uniprot.store.spark.indexer.common.exception;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
public class SolrIndexException extends RuntimeException {

    public SolrIndexException(String message, Throwable cause) {
        super(message, cause);
    }
}
