package org.uniprot.store.datastore.voldemort;

public class DataStoreException extends RuntimeException {
    private static final long serialVersionUID = -7707907761403059158L;

    public DataStoreException(String message) {
        super(message);
    }

    public DataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
