package org.uniprot.store.config.common;

public class FieldValidationException extends RuntimeException {

    public FieldValidationException(String errorMessage) {
        super(errorMessage);
    }

    public FieldValidationException(Throwable err) {
        super(err);
    }

    public FieldValidationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
