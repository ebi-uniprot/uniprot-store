package org.uniprot.store.datastore.voldemort.uniparc;

import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

/**
 * @author lgonzales
 * @since 2020-01-26
 */
public class VoldemortInMemoryUniParcEntryStore extends VoldemortInMemoryEntryStore<UniParcEntry> {

    private static VoldemortInMemoryUniParcEntryStore instance;

    public static VoldemortInMemoryUniParcEntryStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniParcEntryStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniParcEntryStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniParcEntry entry) {
        return entry.getUniParcId().getValue();
    }
}
