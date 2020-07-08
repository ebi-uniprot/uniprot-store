package org.uniprot.store.datastore.voldemort.light.uniref;

import org.uniprot.core.json.parser.uniref.UniRefEntryLightJsonConfig;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortRemoteJsonBinaryStore;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
public class VoldemortRemoteUniRefEntryLightStore
        extends VoldemortRemoteJsonBinaryStore<UniRefEntryLight> {

    public VoldemortRemoteUniRefEntryLightStore(
            int maxConnection, String storeName, String... voldemortUrl) {
        super(maxConnection, storeName, voldemortUrl);
    }

    @Override
    public String getStoreId(UniRefEntryLight entry) {
        return entry.getId().getValue();
    }

    @Override
    public ObjectMapper getStoreObjectMapper() {
        return UniRefEntryLightJsonConfig.getInstance().getFullObjectMapper();
    }

    @Override
    public Class<UniRefEntryLight> getEntryClass() {
        return UniRefEntryLight.class;
    }
}
