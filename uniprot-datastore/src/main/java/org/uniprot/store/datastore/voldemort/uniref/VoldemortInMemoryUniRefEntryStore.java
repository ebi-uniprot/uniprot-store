package org.uniprot.store.datastore.voldemort.uniref;

import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

/**
 * @author jluo
 * @date: 20 Aug 2019
 */
public class VoldemortInMemoryUniRefEntryStore extends VoldemortInMemoryEntryStore<UniRefEntry> {

    private static VoldemortInMemoryUniRefEntryStore instance;

    public static VoldemortInMemoryUniRefEntryStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniRefEntryStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniRefEntryStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniRefEntry entry) {
        return entry.getId().getValue();
    }
}
