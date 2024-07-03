package org.uniprot.store.datastore.voldemort.light.uniparc;

import org.uniprot.core.json.parser.uniparc.UniParcEntryLightJsonConfig;
import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VoldemortRemoteUniParcEntryLightStore
        extends VoldemortRemoteJsonBinaryStore<UniParcEntryLight> {
    public VoldemortRemoteUniParcEntryLightStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcEntryLightStore(
            int maxConnection, boolean brotliEnabled, String storeName, String... voldemortUrl) {
        super(maxConnection, brotliEnabled, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcEntryLightStore(
            int maxConnection,
            boolean brotliEnabled,
            int brotliLevel,
            String storeName,
            String... voldemortUrl) {
        super(maxConnection, brotliEnabled, brotliLevel, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniParcEntryLight entry) {
        return entry.getUniParcId();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniParcEntryLightJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniParcEntryLight> getEntryClass() {
        return UniParcEntryLight.class;
    }
}
