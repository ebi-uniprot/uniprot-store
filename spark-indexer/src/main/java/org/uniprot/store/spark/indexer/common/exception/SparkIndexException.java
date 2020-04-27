package org.uniprot.store.spark.indexer.common.exception;

/**
 * @author lgonzales
 * @since 24/04/2020
 */
public class SparkIndexException extends RuntimeException {

    public SparkIndexException(String message) {
        super(message);
    }

    public SparkIndexException(String message, Throwable cause) {
        super(message, cause);
    }
}
