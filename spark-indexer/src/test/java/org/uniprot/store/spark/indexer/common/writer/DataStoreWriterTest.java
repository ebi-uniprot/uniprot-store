package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.VoldemortInMemoryEntryStore;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;
import org.uniprot.store.spark.indexer.common.store.DataStore;

import voldemort.VoldemortException;

/**
 * @author lgonzales
 * @since 08/05/2020
 */
class DataStoreWriterTest {

    private static String failAccessionId = "P00003";

    @Test
    void indexInStore() {
        try (VoldemortClient<UniProtKBEntry> client =
                VoldemortInMemoryUniprotEntryStore.getInstance(DataStore.UNIPROT.getName())) {
            DataStoreWriter<UniProtKBEntry> dataStoreWriter = new DataStoreWriter<>(client);
            Iterator<UniProtKBEntry> entries = getEntries(3);
            dataStoreWriter.indexInStore(entries);

            assertTrue(client.getEntry("P00001").isPresent());
            assertTrue(client.getEntry("P00002").isPresent());
            assertTrue(client.getEntry("P00003").isPresent());
        }
    }

    @Test
    void indexInStoreRetryThreeTimes() {
        try (FakeVoldemortInMemoryUniprotEntryStore client =
                FakeVoldemortInMemoryUniprotEntryStore.getInstance(DataStore.UNIPROT.getName())) {
            DataStoreWriter<UniProtKBEntry> dataStoreWriter = new DataStoreWriter<>(client);
            Iterator<UniProtKBEntry> entries = getEntries(3);

            assertThrows(VoldemortException.class, () -> dataStoreWriter.indexInStore(entries));

            assertTrue(client.getEntry("P00001").isPresent());
            assertTrue(client.getEntry("P00002").isPresent());
            assertFalse(client.getEntry("P00003").isPresent());

            assertEquals(4, client.getFailCount());
        }
    }

    private Iterator<UniProtKBEntry> getEntries(int end) {
        List<UniProtKBEntry> result = new ArrayList<>();
        IntStream.rangeClosed(1, end)
                .forEach(
                        i -> {
                            UniProtKBEntry entry =
                                    new UniProtKBEntryBuilder(
                                                    "P0000" + i,
                                                    "id" + i,
                                                    UniProtKBEntryType.SWISSPROT)
                                            .build();
                            result.add(entry);
                        });
        return result.iterator();
    }

    private static class FakeVoldemortInMemoryUniprotEntryStore
            extends VoldemortInMemoryEntryStore<UniProtKBEntry> {

        private static FakeVoldemortInMemoryUniprotEntryStore instance;

        private int failCount = 0;

        public static FakeVoldemortInMemoryUniprotEntryStore getInstance(String storeName) {
            if (instance == null) {
                instance = new FakeVoldemortInMemoryUniprotEntryStore(storeName);
            }
            return instance;
        }

        private FakeVoldemortInMemoryUniprotEntryStore(String storeName) {
            super(storeName);
        }

        @Override
        public String getStoreId(UniProtKBEntry entry) {
            return entry.getPrimaryAccession().getValue();
        }

        public int getFailCount() {
            return failCount;
        }

        @Override
        public void saveEntry(UniProtKBEntry entry) {
            String accession = getStoreId(entry);
            if (accession.equals(failAccessionId)) {
                failCount++;
                throw new VoldemortException(
                        "Failed with entry accession: "
                                + getStoreId(entry)
                                + " at index "
                                + failCount);
            } else {
                super.saveEntry(entry);
            }
        }
    }
}
