package org.uniprot.store.datastore.voldemort.light.uniref;

import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

/**
 * @author lgonzales
 * @since 07/07/2020
 */
public class VoldemortInMemoryUniRefEntryLightStore
        extends VoldemortInMemoryEntryStore<UniRefEntryLight> {

    private static VoldemortInMemoryUniRefEntryLightStore instance;

    public static VoldemortInMemoryUniRefEntryLightStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniRefEntryLightStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniRefEntryLightStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniRefEntryLight entry) {
        return entry.getId().getValue();
    }
}
