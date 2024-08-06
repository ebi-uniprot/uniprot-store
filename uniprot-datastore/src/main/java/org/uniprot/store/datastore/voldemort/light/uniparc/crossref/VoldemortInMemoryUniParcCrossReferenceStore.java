package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

public class VoldemortInMemoryUniParcCrossReferenceStore
        extends VoldemortInMemoryEntryStore<UniParcCrossReferencePair> {

    private static VoldemortInMemoryUniParcCrossReferenceStore instance;

    public static VoldemortInMemoryUniParcCrossReferenceStore getInstance(String storeName) {
        if (instance == null) {
            instance = new VoldemortInMemoryUniParcCrossReferenceStore(storeName);
        }
        return instance;
    }

    private VoldemortInMemoryUniParcCrossReferenceStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniParcCrossReferencePair entry) {
        return entry.getKey();
    }

    @Override
    public void saveEntry(String key, UniParcCrossReferencePair entry) {
        doSave(key, entry);
    }
}
