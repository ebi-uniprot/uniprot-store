package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.impl.UniRefEntryLightBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortInMemoryUniRefEntryLightStore;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
class AbstractDataStoreWriterTest {

    @Test
    void canWriteInDataStore() throws Exception {
        FakeAbstractDataStoreWriter writer =
                new FakeAbstractDataStoreWriter("1", "uniref-light", "memory");
        List<UniRefEntryLight> entries = new ArrayList<>();
        UniRefEntryLight entry = new UniRefEntryLightBuilder().id("UnirefId").build();
        entries.add(entry);
        writer.call(entries.iterator());

        UniRefEntryLight storedEntry =
                writer.getDataStoreClient().getEntry("UnirefId").orElseThrow(AssertionError::new);
        assertNotNull(storedEntry);
        assertEquals(entry, storedEntry);
    }

    private static class FakeAbstractDataStoreWriter
            extends AbstractDataStoreWriter<UniRefEntryLight> {

        private static final long serialVersionUID = 6001838489297270762L;

        public FakeAbstractDataStoreWriter(
                String numberOfConnections, String storeName, String connectionURL) {
            super(numberOfConnections, storeName, connectionURL);
        }

        @Override
        protected VoldemortClient<UniRefEntryLight> getDataStoreClient() {
            return VoldemortInMemoryUniRefEntryLightStore.getInstance(storeName);
        }
    }
}
