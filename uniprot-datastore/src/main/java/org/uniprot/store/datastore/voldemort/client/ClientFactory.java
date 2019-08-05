package org.uniprot.store.datastore.voldemort.client;

public interface ClientFactory extends AutoCloseable {
    UniProtClient createUniProtClient();
}
