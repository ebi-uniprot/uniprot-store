package org.uniprot.store.config.schema;

public class SchemaValidationException extends RuntimeException {

    public SchemaValidationException(String errorMessage) {
        super(errorMessage);
    }

    public SchemaValidationException(Throwable err) {
        super(err);
    }

    public SchemaValidationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
