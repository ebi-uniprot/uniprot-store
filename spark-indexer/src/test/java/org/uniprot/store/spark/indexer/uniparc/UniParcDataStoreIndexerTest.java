package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author lgonzales
 * @since 03/12/2020
 */
class UniParcDataStoreIndexerTest {

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
            UniParcDataStoreIndexerTest.FakeUniParcDataStoreIndexer indexer =
                    new UniParcDataStoreIndexerTest.FakeUniParcDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    @Test
    void canGetWriter() {
        DataStoreParameter parameter =
                DataStoreParameter.builder()
                        .maxRetry(1)
                        .delay(1)
                        .connectionURL("tcp://localhost")
                        .numberOfConnections(5)
                        .storeName("uniparc")
                        .build();
        UniParcDataStoreIndexer indexer = new UniParcDataStoreIndexer(null);
        VoidFunction<Iterator<UniParcEntry>> result = indexer.getWriter(parameter);
        assertNotNull(result);
    }

    private static class FakeUniParcDataStoreIndexer extends UniParcDataStoreIndexer {

        public FakeUniParcDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<UniParcEntry>> getWriter(DataStoreParameter parameter) {
            assertNotNull(parameter);
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }
    }
}
