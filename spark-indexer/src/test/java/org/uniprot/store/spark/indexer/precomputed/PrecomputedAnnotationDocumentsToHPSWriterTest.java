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

        PrecomputedAnnotationDocumentsToHPSWriterFake(JobParameter jobParameter) {
            super(jobParameter);
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
                    "UPI0000001866", documentsByAccession.get("UPI0000001866-61156").getUniparc());
            assertEquals(61156, documentsByAccession.get("UPI0000001866-61156").getTaxonomyId());
            assertEquals(
                    "UPI0000001867", documentsByAccession.get("UPI0000001867-10090").getUniparc());
            assertEquals(10090, documentsByAccession.get("UPI0000001867-10090").getTaxonomyId());
            assertEquals(
                    "UPI0000001868", documentsByAccession.get("UPI0000001868-10090").getUniparc());
            assertEquals(10090, documentsByAccession.get("UPI0000001868-10090").getTaxonomyId());
        }
    }
}
