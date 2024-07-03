package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

public class VoldemortInMemoryUniParcCrossReferenceStore
        extends VoldemortInMemoryEntryStore<UniParcCrossReference> {

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
    public String getStoreId(UniParcCrossReference entry) {
        return entry.getId();
    }

    @Override
    public void saveEntry(String key, UniParcCrossReference entry) {
        doSave(key, entry);
    }
}
