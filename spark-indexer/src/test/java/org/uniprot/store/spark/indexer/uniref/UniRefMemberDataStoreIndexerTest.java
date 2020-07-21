package org.uniprot.store.spark.indexer.uniref;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author sahmad
 * @since 21/07/2020
 */
class UniRefMemberDataStoreIndexerTest {

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
            UniRefMemberDataStoreIndexerTest.FakeUniRefMembersDataStoreIndexer indexer =
                    new UniRefMemberDataStoreIndexerTest.FakeUniRefMembersDataStoreIndexer(
                            parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    @Test
    void indexInDataStoreWithError() {
        UniRefMembersDataStoreIndexer indexer = new UniRefMembersDataStoreIndexer(null);
        assertThrows(IndexDataStoreException.class, indexer::indexInDataStore);
    }

    @Test
    void canGetWriter() {
        UniRefLightDataStoreIndexer indexer = new UniRefLightDataStoreIndexer(null);
        VoidFunction<Iterator<UniRefEntryLight>> result =
                indexer.getWriter("5", "uniref-member", "tcp://localhost");
        assertNotNull(result);
    }

    private static class FakeUniRefMembersDataStoreIndexer extends UniRefMembersDataStoreIndexer {

        public FakeUniRefMembersDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<RepresentativeMember>> getWriter(
                String numberOfConnections, String storeName, String connectionURL) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }
    }
}
