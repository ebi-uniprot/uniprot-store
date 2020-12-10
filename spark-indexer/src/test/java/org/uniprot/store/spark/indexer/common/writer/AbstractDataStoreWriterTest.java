package org.uniprot.store.spark.indexer.common.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.impl.UniRefEntryLightBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortInMemoryUniRefEntryLightStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
class AbstractDataStoreWriterTest {

    @Test
    void canWriteInDataStore() throws Exception {
        DataStoreParameter parameter = DataStoreParameter.builder().delay(10L).maxRetry(1).build();
        FakeAbstractDataStoreWriter writer = new FakeAbstractDataStoreWriter(parameter);
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

        public FakeAbstractDataStoreWriter(DataStoreParameter parameter) {
            super(parameter);
        }

        @Override
        protected VoldemortClient<UniRefEntryLight> getDataStoreClient() {
            return VoldemortInMemoryUniRefEntryLightStore.getInstance("uniref-light");
        }
    }
}
