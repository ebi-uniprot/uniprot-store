package org.uniprot.store.spark.indexer.literature;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;
import org.uniprot.core.citation.JournalArticle;
import org.uniprot.core.citation.Literature;
import org.uniprot.core.json.parser.literature.LiteratureJsonConfig;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.store.search.document.DocumentConversionException;
import org.uniprot.store.search.document.literature.LiteratureDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 31/03/2021
 */
class LiteratureDocumentsToHDFSWriterTest {

    @Test
    void writeIndexDocumentsToHDFS() throws IOException {
        Config application = SparkUtils.loadApplicationProperty();
        try (JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application)) {
            JobParameter parameter =
                    JobParameter.builder()
                            .applicationConfig(application)
                            .releaseName("2020_02")
                            .sparkContext(sparkContext)
                            .build();

            LiteratureDocumentsToHDFSWriterTest.LiteratureDocumentsToHDFSWriterFake writer =
                    new LiteratureDocumentsToHDFSWriterTest.LiteratureDocumentsToHDFSWriterFake(
                            parameter);
            writer.writeIndexDocumentsToHDFS();
            List<LiteratureDocument> savedDocuments = writer.getSavedDocuments();
            assertNotNull(savedDocuments);
            assertEquals(15, savedDocuments.size());

            // test entries with only literature
            LiteratureDocument onlyLiteratureDoc =
                    savedDocuments.stream()
                            .filter(doc -> doc.getDocumentId().equals("60"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertNotNull(onlyLiteratureDoc);
            validateOnlyLiteratureDocument(onlyLiteratureDoc);

            // test entries found in entry and literature file
            LiteratureDocument uniprotkbAndLiteratureDoc =
                    savedDocuments.stream()
                            .filter(doc -> doc.getDocumentId().equals("21364755"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertNotNull(uniprotkbAndLiteratureDoc);
            validateUniprotkbAndLiteratureDocument(uniprotkbAndLiteratureDoc);

            // test entries with computational and community statistics
            LiteratureDocument commCompAndUniprotKbDoc =
                    savedDocuments.stream()
                            .filter(doc -> doc.getDocumentId().equals("55555555"))
                            .findFirst()
                            .orElseThrow(AssertionError::new);
            assertNotNull(commCompAndUniprotKbDoc);
            validateCommuCompAndUniProtKbDocument(commCompAndUniprotKbDoc);
        }
    }

    private void validateCommuCompAndUniProtKbDocument(LiteratureDocument doc) {
        assertEquals("55555555", doc.getId());
        assertEquals("PLoS Biol.", doc.getJournal());
        LiteratureEntry entry = convertDocument(doc);

        assertNotNull(entry.getCitation());
        assertTrue(entry.getCitation() instanceof JournalArticle);
        JournalArticle lit = (JournalArticle) entry.getCitation();
        assertEquals("55555555", lit.getId());
        assertEquals("E34", lit.getFirstPage());

        assertNotNull(entry.getStatistics());
        assertEquals(1L, entry.getStatistics().getReviewedProteinCount());
        assertEquals(20L, entry.getStatistics().getCommunityMappedProteinCount());
        assertEquals(30L, entry.getStatistics().getComputationallyMappedProteinCount());
    }

    private void validateUniprotkbAndLiteratureDocument(LiteratureDocument doc) {
        assertEquals("21364755", doc.getId());
        assertEquals("10.1371/journal.pone.0017276", doc.getDoi());
        LiteratureEntry entry = convertDocument(doc);

        assertNotNull(entry.getCitation());
        assertTrue(entry.getCitation() instanceof Literature);
        Literature lit = (Literature) entry.getCitation();
        assertEquals("21364755", lit.getId());
        assertEquals("6", lit.getVolume());
        assertNotNull(lit.getLiteratureAbstract());

        assertNotNull(entry.getStatistics());
        assertEquals(1L, entry.getStatistics().getReviewedProteinCount());
    }

    private void validateOnlyLiteratureDocument(LiteratureDocument doc) {
        assertEquals("60", doc.getId());
        assertEquals("Biochim Biophys Acta", doc.getJournal());
        assertNotNull(doc.getLitAbstract());

        LiteratureEntry entry = convertDocument(doc);
        assertNotNull(entry.getCitation());
        assertEquals("60", entry.getCitation().getId());
        assertNotNull(entry.getCitation().getPublicationDate());
        assertEquals("1975", entry.getCitation().getPublicationDate().getValue());

        assertNull(entry.getStatistics());
    }

    private LiteratureEntry convertDocument(LiteratureDocument literatureDocument) {
        try {
            return LiteratureJsonConfig.getInstance()
                    .getFullObjectMapper()
                    .readValue(literatureDocument.getLiteratureObj(), LiteratureEntry.class);
        } catch (IOException e) {
            throw new DocumentConversionException(
                    "Unable to convert literature entry for documentId: "
                            + literatureDocument.getId(),
                    e);
        }
    }

    private static class LiteratureDocumentsToHDFSWriterFake
            extends LiteratureDocumentsToHDFSWriter {

        private List<LiteratureDocument> documents;

        public LiteratureDocumentsToHDFSWriterFake(JobParameter parameter) {
            super(parameter);
        }

        @Override
        void saveToHDFS(JavaRDD<LiteratureDocument> literatureDocument) {
            documents = literatureDocument.collect();
        }

        List<LiteratureDocument> getSavedDocuments() {
            return documents;
        }
    }
}
