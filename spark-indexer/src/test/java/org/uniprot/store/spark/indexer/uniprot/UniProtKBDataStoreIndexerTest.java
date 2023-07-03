package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.evidence.Evidence;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.store.datastore.voldemort.uniprot.VoldemortInMemoryUniprotEntryStore;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 30/07/2020
 */
class UniProtKBDataStoreIndexerTest {

    @Test
    void indexInDataStore() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            UniProtKBDataStoreIndexerTest.FakeUniProtKBDataStoreIndexer indexer =
                    new UniProtKBDataStoreIndexerTest.FakeUniProtKBDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
            VoldemortInMemoryUniprotEntryStore client =
                    VoldemortInMemoryUniprotEntryStore.getInstance("uniprotkb");
            assertNotNull(client);
            UniProtKBEntry entry = client.getEntry("Q9EPI6").orElseThrow(AssertionError::new);
            assertNotNull(entry);

            // Join UniParc correctly
            assertNotNull(entry.getExtraAttributes());
            assertEquals(3, entry.getExtraAttributes().size());
            assertEquals(
                    "UPI00000E8551",
                    entry.getExtraAttributes().get(UniProtKBEntryBuilder.UNIPARC_ID_ATTRIB));

            // Join go Evidences correctly
            assertNotNull(entry.getUniProtCrossReferencesByType("GO"));

            List<UniProtKBCrossReference> goEvidences = entry.getUniProtCrossReferencesByType("GO");
            assertNotNull(goEvidences);

            UniProtKBCrossReference go5635 =
                    goEvidences.stream()
                            .filter(crossRef -> crossRef.getId().equals("GO:0005635"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);

            assertTrue(go5635.hasEvidences());
            assertEquals(1, go5635.getEvidences().size());
            Evidence evidence = go5635.getEvidences().get(0);
            assertEquals("ECO:0000314", evidence.getEvidenceCode().getCode());
            assertEquals("18303947", evidence.getEvidenceCrossReference().getId());
            assertEquals("PubMed", evidence.getEvidenceCrossReference().getDatabase().getName());

            UniProtKBCrossReference go5634 =
                    goEvidences.stream()
                            .filter(crossRef -> crossRef.getId().equals("GO:0005634"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);

            assertTrue(go5634.hasEvidences());
            assertEquals(3, go5634.getEvidences().size());
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
                        .storeName("uniprot")
                        .build();
        UniProtKBDataStoreIndexer indexer = new UniProtKBDataStoreIndexer(null);
        VoidFunction<Iterator<UniProtKBEntry>> result = indexer.getWriter(parameter);
        assertNotNull(result);
    }

    private static class FakeUniProtKBDataStoreIndexer extends UniProtKBDataStoreIndexer {

        public FakeUniProtKBDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        VoidFunction<Iterator<UniProtKBEntry>> getWriter(DataStoreParameter parameter) {
            return entryIterator -> {
                assertNotNull(entryIterator);
                while (entryIterator.hasNext()) {
                    VoldemortInMemoryUniprotEntryStore.getInstance("uniprotkb")
                            .saveEntry(entryIterator.next());
                }
            };
        }
    }
}
