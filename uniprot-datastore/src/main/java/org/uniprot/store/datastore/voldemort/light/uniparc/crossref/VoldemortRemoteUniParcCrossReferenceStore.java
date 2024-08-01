package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import java.util.List;

import org.uniprot.core.json.parser.uniparc.UniParcCrossRefJsonConfig;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.util.Pair;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

public class VoldemortRemoteUniParcCrossReferenceStore
        extends VoldemortRemoteJsonBinaryStore<Pair<String, List<UniParcCrossReference>>> {
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
    public String getStoreId(Pair<String, List<UniParcCrossReference>> entry) {
        return entry.getKey();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniParcCrossRefJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<Pair<String, List<UniParcCrossReference>>> getEntryClass() {
        return (Class<Pair<String, List<UniParcCrossReference>>>) (Object) Pair.class;
    }

    @Override
    public void saveEntry(String key, Pair<String, List<UniParcCrossReference>> entry) {
        super.doSave(key, entry);
    }
}
