package org.uniprot.store.spark.indexer.uniparc;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.uniparc.UniParcDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 22/06/2020
 */
class UniParcDocumentsToHPSWriterTest {

    @Test
    void writeIndexDocumentsToHPS() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniParcDocumentsToHPSWriterTest.UniParcDocumentsToHPSWriterFake writer =
                    new UniParcDocumentsToHPSWriterTest.UniParcDocumentsToHPSWriterFake(parameter);
            writer.writeIndexDocumentsToHPS();
            List<UniParcDocument> savedDocuments = writer.getSavedDocuments();
            assertNotNull(savedDocuments);
            assertEquals(2, savedDocuments.size());
            UniParcDocument uniParcDocument = savedDocuments.get(0);
            assertEquals("UPI00000E8551", uniParcDocument.getUpi());
            assertEquals(
                    Set.of("01AEF4B6A09EB753", "0A5293FF0AF8EF6FB9A94942D835DAFC"),
                    uniParcDocument.getSequenceChecksums());
            assertTrue(uniParcDocument.getTaxLineageIds().contains(100));
            assertTrue(uniParcDocument.getOrganismTaxons().contains("lineageSC"));
            assertEquals(1, uniParcDocument.getOrganismIds().size());
            assertEquals(Set.of(10116), uniParcDocument.getOrganismIds());

            uniParcDocument = savedDocuments.get(1);
            assertEquals("UPI000000017F", uniParcDocument.getUpi());
        }
    }

    private static class UniParcDocumentsToHPSWriterFake extends UniParcDocumentsToHPSWriter {

        private final JobParameter parameter;
        private List<UniParcDocument> documents;

        public UniParcDocumentsToHPSWriterFake(JobParameter parameter) {
            super(parameter);
            this.parameter = parameter;
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

            return parameter.getSparkContext().parallelizePairs(tuple2List);
        }

        @Override
        void saveToHPS(JavaRDD<UniParcDocument> uniParcDocumentRDD) {
            documents = uniParcDocumentRDD.collect();
        }

        List<UniParcDocument> getSavedDocuments() {
            return documents;
        }
    }
}
