package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.Iterator;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

public class GoogleUniProtKBDataStoreIndexerTest {
    private static final String GOOGLE_PROTLM_STORE_NAME = "google-protlm";

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
            // get after loading in voldermot
            VoldemortInMemoryUniprotEntryStore client =
                    VoldemortInMemoryUniprotEntryStore.getInstance(GOOGLE_PROTLM_STORE_NAME);
            assertNotNull(client);
            UniProtKBEntry entry1 = client.getEntry("A0A6A5BR32").orElseThrow(AssertionError::new);
            assertNotNull(entry1);
            UniProtKBEntry entry2 = client.getEntry("A0A8C6XQ33").orElseThrow(AssertionError::new);
            assertNotNull(entry2);
        }
    }

    static class FakeGoogleUniProtKBDataStoreIndexer extends GoogleUniProtKBDataStoreIndexer {
        public FakeGoogleUniProtKBDataStoreIndexer(JobParameter parameter) {
            super(parameter);
        }

        @Override
        VoidFunction<Iterator<UniProtKBEntry>> getDataStoreWriter(DataStoreParameter parameter) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                while (entryIterator.hasNext()) {
                    VoldemortInMemoryUniprotEntryStore.getInstance(GOOGLE_PROTLM_STORE_NAME)
                            .saveEntry(entryIterator.next());
                }
            };
        }
    }
}
