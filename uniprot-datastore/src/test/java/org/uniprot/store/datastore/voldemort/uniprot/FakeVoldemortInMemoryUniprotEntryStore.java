package org.uniprot.store.datastore.voldemort.uniprot;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.uniprot.core.uniprotkb.UniProtkbEntry;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;

import voldemort.VoldemortException;

public class FakeVoldemortInMemoryUniprotEntryStore
        extends VoldemortInMemoryEntryStore<UniProtkbEntry> {

    private static FakeVoldemortInMemoryUniprotEntryStore instance;
    private static final Logger logger = LoggerFactory.getLogger(VoldemortInMemoryEntryStore.class);
    private int errorFactor = 10;
    private int counter = 0;
    List<String> failedEntries = new ArrayList<String>();

    public static FakeVoldemortInMemoryUniprotEntryStore getInstance(String storeName) {
        if (instance == null) {
            instance = new FakeVoldemortInMemoryUniprotEntryStore(storeName);
        }
        return instance;
    }

    public void setErrorFactor(int errorFactor) {
        this.errorFactor = errorFactor;
    }

    private FakeVoldemortInMemoryUniprotEntryStore(String storeName) {
        super(storeName);
    }

    @Override
    public String getStoreId(UniProtkbEntry entry) {
        return entry.getPrimaryAccession().getValue();
    }

    @Override
    public void saveEntry(UniProtkbEntry entry) {
        counter++;
        if (counter % errorFactor == 0) {
            failedEntries.add(getStoreId(entry));
            throw new VoldemortException(
                    "Fake error with entry accession: "
                            + getStoreId(entry)
                            + " at index "
                            + counter);
        } else {
            super.saveEntry(entry);
        }
    }
}
