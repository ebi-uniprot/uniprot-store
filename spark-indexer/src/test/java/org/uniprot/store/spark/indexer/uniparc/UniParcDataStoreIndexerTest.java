package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

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

    private static class FakeUniParcDataStoreIndexer extends UniParcDataStoreIndexer {

        private final JobParameter jobParameter;

        public FakeUniParcDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
            this.jobParameter = jobParameter;
        }

        @Override
        void saveInDataStore(JavaRDD<UniParcEntry> uniparcJoinedRDD) {
            List<UniParcEntry> result = uniparcJoinedRDD.collect();
            assertNotNull(result);
            assertEquals(2, result.size());
            UniParcEntry entry = result.get(0);
            assertEquals("UPI00000E8551", entry.getUniParcId().getValue());
            entry.getUniParcCrossReferences().stream()
                    .map(UniParcCrossReference::getOrganism)
                    .filter(Objects::nonNull)
                    .forEach(
                            organism -> {
                                // testing join with taxonomy...
                                assertTrue(organism.getTaxonId() > 0);
                                assertFalse(organism.getScientificName().isEmpty());
                            });

            entry = result.get(1);
            assertEquals("UPI000000017F", entry.getUniParcId().getValue());
        }

        @Override
        JavaPairRDD<String, TaxonomyEntry> loadTaxonomyEntryJavaPairRDD() {
            List<Tuple2<String, TaxonomyEntry>> tuple2List = new ArrayList<>();
            TaxonomyEntry tax =
                    new TaxonomyEntryBuilder().taxonId(337687).scientificName("sn337687").build();
            tuple2List.add(new Tuple2<>("337687", tax));

            TaxonomyLineage lineage =
                    new TaxonomyLineageBuilder().taxonId(100).scientificName("lineageSC").build();
            tax =
                    new TaxonomyEntryBuilder()
                            .taxonId(10116)
                            .scientificName("sn10116")
                            .lineagesAdd(lineage)
                            .build();
            tuple2List.add(new Tuple2<>("10116", tax));

            return jobParameter.getSparkContext().parallelizePairs(tuple2List);
        }
    }
}
