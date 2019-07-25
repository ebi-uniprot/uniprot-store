package uk.ac.ebi.uniprot.datastore.voldemort;

public class RetrievalException extends RuntimeException {
    public RetrievalException(){
        super();
    }

    public RetrievalException(String message) {
        super(message);
    }

    public RetrievalException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetrievalException(Throwable cause) {
        super(cause);
    }
}
