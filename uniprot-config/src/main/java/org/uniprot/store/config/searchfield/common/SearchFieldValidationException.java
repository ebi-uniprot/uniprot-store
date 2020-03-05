package org.uniprot.store.config.searchfield.common;

public class SearchFieldValidationException extends RuntimeException {

    public SearchFieldValidationException(String errorMessage) {
        super(errorMessage);
    }

    public SearchFieldValidationException(Throwable err) {
        super(err);
    }

    public SearchFieldValidationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
