package org.uniprot.store.spark.indexer.uniref.writer;

import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.core.uniref.impl.UniRefEntryLightBuilder;
import org.uniprot.store.datastore.voldemort.VoldemortClient;
import org.uniprot.store.datastore.voldemort.light.uniref.VoldemortInMemoryUniRefEntryLightStore;
import org.uniprot.store.datastore.voldemort.uniref.VoldemortInMemoryUniRefEntryStore;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
class UniRefLightDataStoreWriterTest {

    @Test
    void canWriteInDataStore() throws Exception {
        UniRefEntryLight light = new UniRefEntryLightBuilder().id("ID").build();
        FakeUniRefLightDataStoreWriter writer = new FakeUniRefLightDataStoreWriter("5", "light","url");
        writer.call(Collections.singleton(light).iterator());

        Optional<UniRefEntryLight> result = writer.getDataStoreClient().getEntry("ID");
        assertTrue(result.isPresent());
    }

    private static class FakeUniRefLightDataStoreWriter extends UniRefLightDataStoreWriter{

        public FakeUniRefLightDataStoreWriter(String numberOfConnections, String storeName, String connectionURL) {
            super(numberOfConnections, storeName, connectionURL);
        }

        @Override
        VoldemortClient<UniRefEntryLight> getDataStoreClient() {
            return VoldemortInMemoryUniRefEntryLightStore.getInstance("uniref-light");
        }
    }
}