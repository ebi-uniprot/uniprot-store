package org.uniprot.store.datastore.voldemort.client.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.ClientFactory;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import com.google.inject.name.Named;

public class DefaultClientFactory implements ClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientFactory.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    private final VoldemortClient<UniProtKBEntry> voldemortStore;
    private static final String VOLDEMORT_STORE = "avro-uniprot";

    public DefaultClientFactory(@Named("VoldemortURL") String voldemortUrl) {
        this(voldemortUrl, DEFAULT_MAX_CONNECTION);
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn) {
        this(voldemortUrl, numberOfConn, VOLDEMORT_STORE);
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn, String storeName) {
        VoldemortClient<UniProtKBEntry> store = null;
        try {
            store = new VoldemortRemoteUniProtKBEntryStore(numberOfConn, storeName, voldemortUrl);
        } catch (RuntimeException e) {
            LOGGER.error("Unable to get the store", e);
        }
        this.voldemortStore = store;
    }

    @Override
    public UniProtClient createUniProtClient() {
        if (voldemortStore == null) {
            throw new RuntimeException("Voldemort Store initialization failed.");
        }
        return new UniProtClientImpl(voldemortStore);
    }

    @Override
    public void close() {
        //    if(voldemortStore !=null)
        //         voldemortStore.close();

    }
}
