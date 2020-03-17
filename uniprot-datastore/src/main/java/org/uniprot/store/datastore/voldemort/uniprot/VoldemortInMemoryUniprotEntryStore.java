package org.uniprot.store.datastore.voldemort.uniprot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

import voldemort.store.StorageEngine;

/**
 * Use an in Memory storage to save Uniprot entries.
 *
 * <p>Created 05/10/2017
 *
 * @author lgonzales
 */
public class VoldemortInMemoryUniprotEntryStore
        extends VoldemortInMemoryEntryStore<UniProtKBEntry> {

    private static VoldemortInMemoryUniprotEntryStore instance;

    private static final Logger logger =
            LoggerFactory.getLogger(VoldemortInMemoryUniprotEntryStore.class);

    private StorageEngine<String, UniProtKBEntry, String> storageEngine;

    public static VoldemortInMemoryUniprotEntryStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniprotEntryStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniprotEntryStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniProtKBEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }
}
