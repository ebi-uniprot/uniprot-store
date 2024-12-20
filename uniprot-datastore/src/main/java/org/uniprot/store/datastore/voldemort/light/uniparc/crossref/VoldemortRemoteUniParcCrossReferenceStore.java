package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import org.uniprot.core.json.parser.uniparc.UniParcCrossRefJsonConfig;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VoldemortRemoteUniParcCrossReferenceStore
        extends VoldemortRemoteJsonBinaryStore<UniParcCrossReferencePair> {
    public VoldemortRemoteUniParcCrossReferenceStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcCrossReferenceStore(
            int maxConnection, boolean brotliEnabled, String storeName, String... voldemortUrl) {
        super(maxConnection, brotliEnabled, storeName, voldemortUrl);
    }

    public VoldemortRemoteUniParcCrossReferenceStore(
            int maxConnection,
            boolean brotliEnabled,
            int brotliLevel,
            String storeName,
            String... voldemortUrl) {
        super(maxConnection, brotliEnabled, brotliLevel, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniParcCrossReferencePair entry) {
        return entry.getKey();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniParcCrossRefJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniParcCrossReferencePair> getEntryClass() {
        return UniParcCrossReferencePair.class;
    }
}
