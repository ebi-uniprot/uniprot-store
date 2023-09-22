package org.uniprot.store.datastore.voldemort.uniparc;

import org.uniprot.core.json.parser.uniparc.UniParcJsonConfig;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 2020-01-26
 */
public class VoldemortRemoteUniParcEntryStore extends VoldemortRemoteJsonBinaryStore<UniParcEntry> {

    public VoldemortRemoteUniParcEntryStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcEntryStore(
            int maxConnection, boolean brotliEnabled, String storeName, String... voldemortUrl) {
        super(maxConnection, brotliEnabled, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcEntryStore(
            int maxConnection,
            boolean brotliEnabled,
            int brotliLevel,
            String storeName,
            String... voldemortUrl) {
        super(maxConnection, brotliEnabled, brotliLevel, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniParcEntry entry) {
        return entry.getUniParcId().getValue();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniParcJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniParcEntry> getEntryClass() {
        return UniParcEntry.class;
    }
}
