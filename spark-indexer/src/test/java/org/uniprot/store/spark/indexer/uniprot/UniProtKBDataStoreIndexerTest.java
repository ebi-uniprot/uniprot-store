package org.uniprot.store.spark.indexer.uniprot;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import java.util.Iterator;
import java.util.ResourceBundle;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
class UniProtKBDataStoreIndexerTest {

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
            UniProtKBDataStoreIndexerTest.FakeUniProtKBDataStoreIndexer indexer = new UniProtKBDataStoreIndexerTest.FakeUniProtKBDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    @Test
    void canGetWriter() {
        UniProtKBDataStoreIndexer indexer = new UniProtKBDataStoreIndexer(null);
        VoidFunction<Iterator<UniProtKBEntry>> result =
                indexer.getWriter("5", "uniprot", "tcp://localhost");
        assertNotNull(result);
    }

    private static class FakeUniProtKBDataStoreIndexer extends UniProtKBDataStoreIndexer {

        public FakeUniProtKBDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<UniProtKBEntry>> getWriter(
                String numberOfConnections, String storeName, String connectionURL) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }
    }
}