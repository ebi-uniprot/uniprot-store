package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

class GoogleUniProtKBDataStoreIndexerTest {

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
            GoogleUniProtKBDataStoreIndexer indexer =
                    new FakeGoogleUniProtKBDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
        }
    }

    static class FakeGoogleUniProtKBDataStoreIndexer extends GoogleUniProtKBDataStoreIndexer {
        public FakeGoogleUniProtKBDataStoreIndexer(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveInDataStore(JavaRDD<UniProtKBEntry> protLMEntryRDD) {
            List<UniProtKBEntry> protLMEntries = protLMEntryRDD.collect();
            assertNotNull(protLMEntries);
            assertEquals(2, protLMEntries.size());
            assertEquals("A0A6A5BR32", protLMEntries.get(0).getPrimaryAccession().getValue());
            assertEquals("A0A8C6XQ33", protLMEntries.get(1).getPrimaryAccession().getValue());
        }

        @Override
        JavaRDD<UniProtKBEntry> joinRDDPairs(
                JavaPairRDD<String, UniProtKBEntry> protLMPairRDD,
                JavaPairRDD<String, UniProtKBEntry> uniProtRDDPair) {
            return protLMPairRDD.values();
        }
    }
}
