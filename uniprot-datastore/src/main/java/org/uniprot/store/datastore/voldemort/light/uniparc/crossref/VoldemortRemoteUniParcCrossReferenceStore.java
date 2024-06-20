package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import org.uniprot.core.json.parser.uniparc.UniParcCrossRefJsonConfig;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VoldemortRemoteUniParcCrossReferenceStore
        extends VoldemortRemoteJsonBinaryStore<UniParcCrossReference> {
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
    public String getStoreId(UniParcCrossReference entry) {
        return entry.getId();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniParcCrossRefJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniParcCrossReference> getEntryClass() {
        return UniParcCrossReference.class;
    }
}
