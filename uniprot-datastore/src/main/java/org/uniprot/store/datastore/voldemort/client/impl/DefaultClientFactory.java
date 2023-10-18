package org.uniprot.store.datastore.voldemort.client.impl;

import static org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore.BROTLI_ENABLED;
import static org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore.DEFAULT_BROTLI_COMPRESSION_LEVEL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.client.ClientFactory;
import org.uniprot.store.datastore.voldemort.client.UniProtClient;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortRemoteUniProtKBEntryStore;

import com.google.inject.name.Named;

/**
 * Warning - This class is being used by AA team so if you make any change in {@link
 * VoldemortRemoteUniProtKBEntryStore} make sure it is backward compatible. If it is breaking change
 * please co-ordinate with AA team.
 */
public class DefaultClientFactory implements ClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultClientFactory.class);
    private static final int DEFAULT_MAX_CONNECTION = 20;
    private static final String VOLDEMORT_STORE = "avro-uniprot";

    private final VoldemortClient<UniProtKBEntry> voldemortClient;

    public DefaultClientFactory(@Named("VoldemortURL") String voldemortUrl) {
        this(voldemortUrl, DEFAULT_MAX_CONNECTION);
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn) {
        this(voldemortUrl, numberOfConn, VOLDEMORT_STORE);
    }

    public DefaultClientFactory(String voldemortUrl, int numberOfConn, String storeName) {
        this(
                voldemortUrl,
                numberOfConn,
                storeName,
                BROTLI_ENABLED,
                DEFAULT_BROTLI_COMPRESSION_LEVEL);
    }

    public DefaultClientFactory(
            String voldemortUrl,
            int numberOfConn,
            String storeName,
            boolean brotliEnabled,
            int brotliCompressionLevel) {
        VoldemortClient<UniProtKBEntry> vdClient = null;
        try {
            vdClient =
                    new VoldemortRemoteUniProtKBEntryStore(
                            numberOfConn,
                            brotliEnabled,
                            brotliCompressionLevel,
                            storeName,
                            voldemortUrl);
        } catch (RuntimeException e) {
            LOGGER.error("Unable to get the store", e);
        }
        this.voldemortClient = vdClient;
    }

    @Override
    public UniProtClient createUniProtClient() {
        if (this.voldemortClient == null) {
            throw new RuntimeException("Voldemort Store initialization failed.");
        }
        return new UniProtClientImpl(this.voldemortClient);
    }

    @Override
    public void close() {
        // do nothing
    }
}
