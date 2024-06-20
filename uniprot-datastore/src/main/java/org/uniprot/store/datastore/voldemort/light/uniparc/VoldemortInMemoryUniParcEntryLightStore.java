package org.uniprot.store.datastore.voldemort.light.uniparc;

import org.uniprot.core.uniparc.UniParcEntryLight;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

public class VoldemortInMemoryUniParcEntryLightStore
        extends VoldemortInMemoryEntryStore<UniParcEntryLight> {
    private static VoldemortInMemoryUniParcEntryLightStore instance;

    public static VoldemortInMemoryUniParcEntryLightStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniParcEntryLightStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniParcEntryLightStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniParcEntryLight entry) {
        return entry.getUniParcId().getValue();
    }
}
