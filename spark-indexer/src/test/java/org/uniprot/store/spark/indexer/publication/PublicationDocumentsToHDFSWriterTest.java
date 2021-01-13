package org.uniprot.store.spark.indexer.publication;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.util.List;
import java.util.ResourceBundle;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.publication.PublicationDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

/**
 * @author sahmad
 * @created 15/12/2020
 */
public class PublicationDocumentsToHDFSWriterTest {
    @Test
    void writeIndexDocumentsToHDFS() throws IOException {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            PublicationDocumentsToHDFSWriterTest.PublicationDocumentsToHDFSWriterFake writer =
                    new PublicationDocumentsToHDFSWriterTest.PublicationDocumentsToHDFSWriterFake(
                            parameter);
            writer.writeIndexDocumentsToHDFS();
            List<PublicationDocument> savedDocuments = writer.getSavedDocuments();
            assertNotNull(savedDocuments);
            assertEquals(7, savedDocuments.size());
        }
    }

    private static class PublicationDocumentsToHDFSWriterFake
            extends PublicationDocumentsToHDFSWriter {
        private List<PublicationDocument> documents;

        public PublicationDocumentsToHDFSWriterFake(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveToHDFS(JavaRDD<PublicationDocument> publicationDocumentRDD) {
            documents = publicationDocumentRDD.collect();
        }

        List<PublicationDocument> getSavedDocuments() {
            return documents;
        }
    }
}
