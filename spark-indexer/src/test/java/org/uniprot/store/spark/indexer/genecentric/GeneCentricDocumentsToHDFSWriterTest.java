package org.uniprot.store.spark.indexer.genecentric;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.uniprot.core.fasta.ProteinFasta;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.store.search.document.genecentric.GeneCentricDocument;
import org.uniprot.store.search.document.genecentric.GeneCentricDocumentConverter;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 22/10/2020
 */
class GeneCentricDocumentsToHDFSWriterTest {

    @Test
    void writeIndexDocumentsToHDFS() throws IOException {
        ObjectMapper objectMapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();
        GeneCentricDocumentConverter converter = new GeneCentricDocumentConverter(objectMapper);
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            GeneCentricDocumentsToHDFSWriterFake writer =
                    new GeneCentricDocumentsToHDFSWriterFake(parameter);
            writer.writeIndexDocumentsToHDFS();
            List<GeneCentricDocument> savedDocuments = writer.getSavedDocuments();
            assertNotNull(savedDocuments);
            assertEquals(40, savedDocuments.size());

            // UP000000554 join worked canonical: O51971 related: O51971
            returnsCanonicalWithRelated(converter, savedDocuments);

            // UP000478052 join worked canonical: A0A6G0Z6X6, related:A0A6G0Z6T5, A0A6G0Z7D3
            returnCanonicalWithMultipleRelated(converter, savedDocuments);

            // Left Join Worked: canonical:A0A6G0ZDD9  no Related
            returnsCanonicalWithoutRelated(converter, savedDocuments);
        }
    }

    private void returnsCanonicalWithRelated(
            GeneCentricDocumentConverter converter, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("O51971"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("O51971", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = converter.getCanonicalEntryFromDocument(document);
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000000554", entry.getProteomeId());
        assertEquals("O51971", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertEquals(1, entry.getRelatedProteins().size());
        Protein relatedProtein = entry.getRelatedProteins().get(0);
        assertEquals("Q9HI14", relatedProtein.getId());
    }

    private void returnCanonicalWithMultipleRelated(
            GeneCentricDocumentConverter converter, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("A0A6G0Z6X6"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("A0A6G0Z6X6", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = converter.getCanonicalEntryFromDocument(document);
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000478052", entry.getProteomeId());
        assertEquals("A0A6G0Z6X6", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertEquals(2, entry.getRelatedProteins().size());
        List<String> relatedAccessions =
                entry.getRelatedProteins().stream()
                        .map(ProteinFasta::getId)
                        .collect(Collectors.toList());
        assertTrue(relatedAccessions.contains("A0A6G0Z6T5"));
        assertTrue(relatedAccessions.contains("A0A6G0Z7D3"));
    }

    private void returnsCanonicalWithoutRelated(
            GeneCentricDocumentConverter converter, List<GeneCentricDocument> savedDocuments) {
        GeneCentricDocument document =
                savedDocuments.stream()
                        .filter(doc -> doc.getAccession().equals("A0A6G0ZDD9"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals("A0A6G0ZDD9", document.getAccession());
        assertNotNull(document.getGeneCentricStored());

        GeneCentricEntry entry = converter.getCanonicalEntryFromDocument(document);
        assertNotNull(entry.getCanonicalProtein());
        assertEquals("UP000478052", entry.getProteomeId());
        assertEquals("A0A6G0ZDD9", entry.getCanonicalProtein().getId());

        assertNotNull(entry.getRelatedProteins());
        assertTrue(entry.getRelatedProteins().isEmpty());
    }

    private static class GeneCentricDocumentsToHDFSWriterFake
            extends GeneCentricDocumentsToHDFSWriter {

        private List<GeneCentricDocument> documents;

        public GeneCentricDocumentsToHDFSWriterFake(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveToHDFS(JavaRDD<GeneCentricDocument> geneCentricDocument) {
            documents = geneCentricDocument.collect();
        }

        List<GeneCentricDocument> getSavedDocuments() {
            return documents;
        }
    }
}
