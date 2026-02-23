package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

public class PrecomputedAnnotationDataStoreIndexerTest {
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
            PrecomputedAnnotationDataStoreIndexer indexer =
                    new FakePrecomputedAnnotationDataStoreIndexer(parameter);
            indexer.indexInDataStore();
        }
    }

    static class FakePrecomputedAnnotationDataStoreIndexer
            extends PrecomputedAnnotationDataStoreIndexer {
        public FakePrecomputedAnnotationDataStoreIndexer(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveInDataStore(JavaRDD<UniProtKBEntry> uniProtKBEntryRDD) {
            List<UniProtKBEntry> entries = uniProtKBEntryRDD.collect();
            assertNotNull(entries);
            assertEquals(3, entries.size());
            assertEquals("UPI0000001866-61156", entries.get(0).getPrimaryAccession().getValue());
            assertEquals("AA", entries.get(0).getEntryType().getName());
            assertEquals("UPI0000001867-10090", entries.get(1).getPrimaryAccession().getValue());
            assertEquals("AA", entries.get(1).getEntryType().getName());
            assertEquals("UPI0000001868-10090", entries.get(2).getPrimaryAccession().getValue());
            assertEquals("AA", entries.get(2).getEntryType().getName());
        }
    }
}
