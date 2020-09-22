package org.uniprot.store.spark.indexer.uniref;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Iterator;
import java.util.ResourceBundle;

import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.UniRefEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 20/07/2020
 */
@Slf4j
class UniRefDataStoreIndexerTest {

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
            FakeUniRefDataStoreIndexer indexer = new FakeUniRefDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    @Test
    void indexInDataStoreWithError() {
        UniRefDataStoreIndexer indexer = new UniRefDataStoreIndexer(null);
        assertThrows(IndexDataStoreException.class, indexer::indexInDataStore);
    }

    @Test
    void canGetWriter() {
        UniRefDataStoreIndexer indexer = new UniRefDataStoreIndexer(null);
        VoidFunction<Iterator<UniRefEntry>> result =
                indexer.getWriter("5", "uniref", "tcp://localhost");
        assertNotNull(result);
    }

    private static class FakeUniRefDataStoreIndexer extends UniRefDataStoreIndexer {

        public FakeUniRefDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<UniRefEntry>> getWriter(
                String numberOfConnections, String storeName, String connectionURL) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }
    }
}
