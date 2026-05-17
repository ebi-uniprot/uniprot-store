package org.uniprot.store.spark.indexer.precomputed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

class PrecomputedAnnotationDocumentsToHPSWriterIT {
    private static final String RELEASE_NAME = "2020_02";

    @Test
    void writeIndexDocumentsToHPSFromPrecomputedAndProteomeFiles() {
        Config application =
                ConfigFactory.parseString(
                                "proteome.xml.file = proteome/proteome-matching-taxonomy.xml")
                        .withFallback(SparkUtils.loadApplicationProperty())
                        .resolve();

        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName(RELEASE_NAME)
                            .sparkContext(sparkContext)
                            .build();
            PrecomputedAnnotationDocumentsToHPSWriterFake writer =
                    new PrecomputedAnnotationDocumentsToHPSWriterFake(parameter);

            writer.writeIndexDocumentsToHPS();

            List<PrecomputedAnnotationDocument> savedDocuments = writer.savedDocuments;
            assertNotNull(savedDocuments);
            assertEquals(3, savedDocuments.size());

            Map<String, PrecomputedAnnotationDocument> documentsByAccession =
                    savedDocuments.stream()
                            .collect(
                                    Collectors.toMap(
                                            PrecomputedAnnotationDocument::getAccession,
                                            document -> document));

            assertEquals(
                    List.of("UP000061156"),
                    documentsByAccession.get("UPI0000001866-61156").getProteome());
            assertEquals(
                    List.of("UP000010090", "UP000010091"),
                    documentsByAccession.get("UPI0000001867-10090").getProteome());
            assertEquals(
                    List.of("UP000010090", "UP000010091"),
                    documentsByAccession.get("UPI0000001868-10090").getProteome());
        }
    }

    private static class PrecomputedAnnotationDocumentsToHPSWriterFake
            extends PrecomputedAnnotationDocumentsToHPSWriter {
        private List<PrecomputedAnnotationDocument> savedDocuments;

        PrecomputedAnnotationDocumentsToHPSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
        }

        @Override
        void saveToHPS(JavaRDD<PrecomputedAnnotationDocument> documentRDD) {
            this.savedDocuments = documentRDD.collect();
        }
    }
}
