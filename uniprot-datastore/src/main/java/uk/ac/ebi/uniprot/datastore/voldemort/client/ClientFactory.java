package uk.ac.ebi.uniprot.datastore.voldemort.client;

public interface ClientFactory extends AutoCloseable {
    UniProtClient createUniProtClient();
}
