package org.uniprot.store.spark.indexer.precomputed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class PrecomputedAnnotationDocumentsToHPSWriterTest {

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
            PrecomputedAnnotationDocumentsToHPSWriterFake writer =
                    new PrecomputedAnnotationDocumentsToHPSWriterFake(parameter);

            writer.writeIndexDocumentsToHPS();
        }
    }

    private static class PrecomputedAnnotationDocumentsToHPSWriterFake
            extends PrecomputedAnnotationDocumentsToHPSWriter {

        private final JobParameter jobParameter;

        PrecomputedAnnotationDocumentsToHPSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
            this.jobParameter = jobParameter;
        }

        @Override
        JavaPairRDD<String, String> loadTaxonomyProteomeIds() {
            return jobParameter
                    .getSparkContext()
                    .parallelizePairs(
                            List.of(
                                    new Tuple2<>("61156", "UP000000001"),
                                    new Tuple2<>("10090", "UP000000002"),
                                    new Tuple2<>("10090", "UP000000003")));
        }

        @Override
        void saveToHPS(JavaRDD<PrecomputedAnnotationDocument> documentRDD) {
            List<PrecomputedAnnotationDocument> documents = documentRDD.collect();

            assertNotNull(documents);
            assertEquals(3, documents.size());
            Map<String, PrecomputedAnnotationDocument> documentsByAccession =
                    documents.stream()
                            .collect(
                                    Collectors.toMap(
                                            PrecomputedAnnotationDocument::getAccession,
                                            document -> document));
            assertEquals(
                    List.of("UP000000001"),
                    documentsByAccession.get("UPI0000001866-61156").getProteome());
            assertEquals(
                    List.of("UP000000002", "UP000000003"),
                    documentsByAccession.get("UPI0000001867-10090").getProteome());
            assertEquals(
                    List.of("UP000000002", "UP000000003"),
                    documentsByAccession.get("UPI0000001868-10090").getProteome());
        }
    }
}
