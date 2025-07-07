package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.impl.UniParcCrossReferencePair;
import org.uniprot.core.uniprotkb.taxonomy.Organism;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.store.DataStoreParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class UniParcCrossReferenceDataStoreIndexerTest {
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
            UniParcCrossReferenceDataStoreIndexerTest.FakeUniParcCrossRefDataStoreIndexer indexer =
                    new UniParcCrossReferenceDataStoreIndexerTest
                            .FakeUniParcCrossRefDataStoreIndexer(parameter);
            assertNotNull(indexer);
            indexer.indexInDataStore();
            DataStoreParameter dataStoreParams =
                    indexer.getDataStoreParameter(parameter.getApplicationConfig());
            assertNotNull(dataStoreParams);
        }
    }

    private static class FakeUniParcCrossRefDataStoreIndexer
            extends UniParcCrossReferenceDataStoreIndexer {

        private final JobParameter jobParameter;

        public FakeUniParcCrossRefDataStoreIndexer(JobParameter jobParameter) {
            super(jobParameter);
            this.jobParameter = jobParameter;
        }

        @Override
        void saveInDataStore(JavaRDD<UniParcCrossReferencePair> uniParcCrossRefWrap) {
            List<UniParcCrossReferencePair> result = uniParcCrossRefWrap.collect();
            assertNotNull(result);
            assertEquals(11, result.size());
            Optional<UniParcCrossReferencePair> firstBatch =
                    result.stream()
                            .filter(pair -> "UPI00000E8551_0".equals(pair.getKey()))
                            .findFirst();
            assertTrue(firstBatch.isPresent());
            List<UniParcCrossReference> batchValues = firstBatch.get().getValue();
            assertNotNull(batchValues);
            assertEquals(3, batchValues.size());
            UniParcCrossReference xref = batchValues.get(0);
            Organism organism = xref.getOrganism();
            assertEquals(10116, organism.getTaxonId());
            assertEquals("sn10116", organism.getScientificName());
            assertEquals("", organism.getCommonName());

            assertNotNull(xref.getProperties());
            assertFalse(xref.getProperties().isEmpty());
            Property source = xref.getProperties().get(0);
            assertEquals(UniParcCrossReference.PROPERTY_SOURCES, source.getKey());
            assertEquals("CAC20866:UP000002494:Chromosome 1", source.getValue());
            // get crossref of UniParc entry without taxonomy id
            Optional<UniParcCrossReferencePair> crossRefBatchWithoutTaxon =
                    result.stream()
                            .filter(pair -> "UPI000028554A_0".equals(pair.getKey()))
                            .findFirst();
            assertTrue(crossRefBatchWithoutTaxon.isPresent());
            List<UniParcCrossReference> crossRefWithoutTaxon =
                    crossRefBatchWithoutTaxon.get().getValue();
            assertEquals(1, crossRefWithoutTaxon.size());
            assertEquals("EAJ82135", crossRefWithoutTaxon.get(0).getId());
            assertEquals(UniParcDatabase.EMBLWGS, crossRefWithoutTaxon.get(0).getDatabase());
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
