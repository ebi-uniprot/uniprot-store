package org.uniprot.store.spark.indexer.uniref.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.core.uniref.impl.UniRefEntryBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortInMemoryUniRefEntryStore;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
class UniRefDataStoreWriterTest {

    @Test
    void canWriteInDataStore() throws Exception {
        UniRefEntry light = new UniRefEntryBuilder().id("ID").build();
        FakeUniRefDataStoreWriter writer = new FakeUniRefDataStoreWriter("5", "uniref", "url");
        writer.call(Collections.singleton(light).iterator());

        Optional<UniRefEntry> result = writer.getDataStoreClient().getEntry("ID");
        assertTrue(result.isPresent());
    }

    private static class FakeUniRefDataStoreWriter extends UniRefDataStoreWriter {

        public FakeUniRefDataStoreWriter(
                String numberOfConnections, String storeName, String connectionURL) {
            super(numberOfConnections, storeName, connectionURL);
        }

        @Override
        VoldemortClient<UniRefEntry> getDataStoreClient() {
            return VoldemortInMemoryUniRefEntryStore.getInstance("uniref");
        }
    }
}
