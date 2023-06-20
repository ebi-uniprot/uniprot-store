package org.uniprot.store.spark.indexer.uniref;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.uniref.UniRefDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import scala.Tuple2;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 15/06/2020
 */
class UniRefDocumentsToHDFSWriterTest {

    @Test
    void writeIndexDocumentsToHDFS() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            UniRefDocumentsToHDFSWriterFake writer = new UniRefDocumentsToHDFSWriterFake(parameter);
            writer.writeIndexDocumentsToHDFS();
            List<UniRefDocument> savedDocuments = writer.getSavedDocuments();
            assertNotNull(savedDocuments);
            assertEquals(3, savedDocuments.size());
            UniRefDocument uniref50 =
                    savedDocuments.stream()
                            .filter(doc -> doc.getId().equals("UniRef50_Q9EPI6"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertEquals("UniRef50_Q9EPI6", uniref50.getId());
            assertEquals(
                    "Cluster: NMDA receptor synaptonuclear signaling and neuronal migration factor",
                    uniref50.getName());
            assertTrue(uniref50.getTaxLineageIds().contains(100));
            assertTrue(uniref50.getOrganismTaxons().contains("lineageSC"));
        }
    }

    private static class UniRefDocumentsToHDFSWriterFake extends UniRefDocumentsToHDFSWriter {

        private final JobParameter parameter;
        private List<UniRefDocument> documents;

        public UniRefDocumentsToHDFSWriterFake(JobParameter parameter) {
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
        void saveToHDFS(JavaRDD<UniRefDocument> unirefDocumentRDD) {
            documents = unirefDocumentRDD.collect();
        }

        List<UniRefDocument> getSavedDocuments() {
            return documents;
        }
    }
}
