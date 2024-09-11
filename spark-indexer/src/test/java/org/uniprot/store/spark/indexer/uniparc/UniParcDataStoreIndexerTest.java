package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 03/12/2020
 */
class UniParcDataStoreIndexerTest {

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
            UniParcDataStoreIndexerTest.FakeUniParcDataStoreIndexer indexer =
                    new UniParcDataStoreIndexerTest.FakeUniParcDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
            DataStoreParameter dataStoreParams =
                    indexer.getDataStoreParameter(parameter.getApplicationConfig());
            assertNotNull(dataStoreParams);
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
            UniParcEntry entry =
                    result.stream()
                            .filter(e -> e.getUniParcId().getValue().equals("UPI00000E8551"))
                            .findFirst()
                            .orElse(null);
            assertNotNull(entry);
            entry.getUniParcCrossReferences().stream()
                    .map(UniParcCrossReference::getOrganism)
                    .filter(Objects::nonNull)
                    .forEach(
                            organism -> {
                                // testing join with taxonomy...
                                assertTrue(organism.getTaxonId() > 0);
                            });
            Optional<Organism> optOrganism =
                    entry.getUniParcCrossReferences().stream()
                            .map(UniParcCrossReference::getOrganism)
                            .filter(org -> Objects.nonNull(org) && org.getTaxonId() == 10116)
                            .findAny();
            assertTrue(optOrganism.isPresent());
            assertFalse(optOrganism.get().getScientificName().isEmpty());
            assertEquals("sn10116", optOrganism.get().getScientificName());

            entry =
                    result.stream()
                            .filter(e -> e.getUniParcId().getValue().equals("UPI000000017F"))
                            .findFirst()
                            .orElse(null);
            assertNotNull(entry);
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
