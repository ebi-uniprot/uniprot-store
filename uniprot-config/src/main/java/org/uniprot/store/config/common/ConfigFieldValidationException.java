package org.uniprot.store.config.common;

public class ConfigFieldValidationException extends RuntimeException {

    public ConfigFieldValidationException(String errorMessage) {
        super(errorMessage);
    }

    public ConfigFieldValidationException(Throwable err) {
        super(err);
    }

    public ConfigFieldValidationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
