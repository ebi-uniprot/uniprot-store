package org.uniprot.store.spark.indexer.uniref;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import java.util.Iterator;
import java.util.ResourceBundle;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
class UniRefLightDataStoreIndexerTest {

    @Test
    void indexInDataStore() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            UniRefLightDataStoreIndexerTest.FakeUniRefLightDataStoreIndexer indexer = new UniRefLightDataStoreIndexerTest.FakeUniRefLightDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    @Test
    void indexInDataStoreWithError() {
        UniRefLightDataStoreIndexer indexer = new UniRefLightDataStoreIndexer(null);
        assertThrows(IndexDataStoreException.class, indexer::indexInDataStore);
    }

    @Test
    void canGetWriter() {
        UniRefLightDataStoreIndexer indexer = new UniRefLightDataStoreIndexer(null);
        VoidFunction<Iterator<UniRefEntryLight>> result = indexer.getWriter("5", "uniref-light", "tcp://localhost");
        assertNotNull(result);
    }

    private static class FakeUniRefLightDataStoreIndexer extends UniRefLightDataStoreIndexer {

        public FakeUniRefLightDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<UniRefEntryLight>> getWriter(String numberOfConnections, String storeName, String connectionURL) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }

    }
}