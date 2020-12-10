package org.uniprot.store.spark.indexer.uniref.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.impl.UniRefEntryLightBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortInMemoryUniRefEntryLightStore;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
class UniRefLightDataStoreWriterTest {

    @Test
    void canWriteInDataStore() throws Exception {
        UniRefEntryLight light = new UniRefEntryLightBuilder().id("ID").build();

        DataStoreParameter parameter = DataStoreParameter.builder().delay(10L).maxRetry(1).build();
        FakeUniRefLightDataStoreWriter writer = new FakeUniRefLightDataStoreWriter(parameter);
        writer.call(Collections.singleton(light).iterator());

        Optional<UniRefEntryLight> result = writer.getDataStoreClient().getEntry("ID");
        assertTrue(result.isPresent());
    }

    private static class FakeUniRefLightDataStoreWriter extends UniRefLightDataStoreWriter {

        private static final long serialVersionUID = 2605332021532353301L;

        public FakeUniRefLightDataStoreWriter(DataStoreParameter parameter) {
            super(parameter);
        }

        @Override
        public VoldemortClient<UniRefEntryLight> getDataStoreClient() {
            return VoldemortInMemoryUniRefEntryLightStore.getInstance("uniref-light");
        }
    }
}
