package org.uniprot.store.datastore.voldemort.light.uniparc.crossref;

import java.util.List;

import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.util.Pair;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

public class VoldemortInMemoryUniParcCrossReferenceStore
        extends VoldemortInMemoryEntryStore<Pair<String, List<UniParcCrossReference>>> {

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
    public String getStoreId(Pair<String, List<UniParcCrossReference>> entry) {
        return entry.getKey();
    }

    @Override
    public void saveEntry(String key, Pair<String, List<UniParcCrossReference>> entry) {
        doSave(key, entry);
    }
}
