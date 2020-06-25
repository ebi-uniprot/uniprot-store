package org.uniprot.store.spark.indexer.uniprot;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniprotkb.UniProtKBEntry;
import org.uniprot.core.uniprotkb.impl.UniProtKBEntryBuilder;
import org.uniprot.core.uniprotkb.xdb.UniProtKBCrossReference;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 25/06/2020
 */
class UniProtKBDataStoreIndexerTest {

    @Test
    void testIndexInDataStore() {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            FakeUniProtKBDataStoreIndexer indexer = new FakeUniProtKBDataStoreIndexer(parameter);
            indexer.indexInDataStore();
            assertEquals(1, indexer.savedEntries.size());
            UniProtKBEntry entry = indexer.savedEntries.get(0);

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

            UniProtKBCrossReference go5634 =
                    goEvidences.stream()
                            .filter(crossRef -> crossRef.getId().equals("GO:0005634"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);

            assertTrue(go5634.hasEvidences());
            assertEquals(3, go5634.getEvidences().size());
        }
    }

    private static class FakeUniProtKBDataStoreIndexer extends UniProtKBDataStoreIndexer {

        List<UniProtKBEntry> savedEntries = new ArrayList<>();

        public FakeUniProtKBDataStoreIndexer(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveInDataStore(JavaPairRDD<String, UniProtKBEntry> uniprotRDD) {
            uniprotRDD.collect().stream().map(Tuple2::_2).forEach(savedEntries::add);
        }
    }
}
