package org.uniprot.store.spark.indexer.precomputed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.precomputed.PrecomputedAnnotationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

import scala.Tuple2;

class PrecomputedAnnotationDocumentRDDReaderTest {

    @Test
    void testLoadPrecomputedAnnotationDocuments() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            PrecomputedAnnotationDocumentRDDReader reader =
                    new PrecomputedAnnotationDocumentRDDReader(parameter);
            JavaPairRDD<String, PrecomputedAnnotationDocument> result = reader.load();
            assertNotNull(result);

            assertEquals(3L, result.count());
            List<Tuple2<String, PrecomputedAnnotationDocument>> documents = result.collect();

            assertEquals(3, documents.size());
            assertEquals("61156", documents.get(0)._1);
            assertEquals("UPI0000001866-61156", documents.get(0)._2.getAccession());
            assertEquals("10090", documents.get(1)._1);
            assertEquals("UPI0000001867-10090", documents.get(1)._2.getAccession());
            assertEquals("10090", documents.get(2)._1);
            assertEquals("UPI0000001868-10090", documents.get(2)._2.getAccession());
            documents.forEach(
                    document -> assertEquals(getTaxonomy(document._2.getAccession()), document._1));
        }
    }

    @Test
    void testLoadPrecomputedAnnotationDocumentValues() {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();
            PrecomputedAnnotationDocumentRDDReader reader =
                    new PrecomputedAnnotationDocumentRDDReader(parameter);

            List<PrecomputedAnnotationDocument> documents = reader.loadDocuments().collect();

            assertEquals(3, documents.size());
            PrecomputedAnnotationDocument firstDocument = documents.get(0);
            assertNotNull(firstDocument);
            assertEquals("UPI0000001866-61156", firstDocument.getAccession());
            assertTrue(firstDocument.getProteome().isEmpty());
        }
    }

    private static String getTaxonomy(String accession) {
        return accession.substring(accession.lastIndexOf('-') + 1);
    }
}
