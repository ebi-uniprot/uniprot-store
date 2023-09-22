package org.uniprot.store.spark.indexer.uniref;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniref.RepresentativeMember;
import org.uniprot.core.uniref.UniRefEntryLight;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author sahmad
 * @since 21/07/2020
 */
class UniRefMemberDataStoreIndexerTest {

    @Test
    void indexInDataStore() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
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
        DataStoreParameter parameter =
                DataStoreParameter.builder()
                        .maxRetry(1)
                        .delay(1)
                        .connectionURL("tcp://localhost")
                        .numberOfConnections(5)
                        .storeName("uniref-member")
                        .build();
        UniRefLightDataStoreIndexer indexer = new UniRefLightDataStoreIndexer(null);
        VoidFunction<Iterator<UniRefEntryLight>> result = indexer.getWriter(parameter);
        assertNotNull(result);
    }

    private static class FakeUniRefMembersDataStoreIndexer extends UniRefMembersDataStoreIndexer {

        public FakeUniRefMembersDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<RepresentativeMember>> getWriter(DataStoreParameter parameter) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                assertTrue(entryIterator.hasNext());
            };
        }
    }
}
