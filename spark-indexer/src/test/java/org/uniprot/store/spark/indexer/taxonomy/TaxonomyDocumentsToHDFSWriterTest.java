package org.uniprot.store.spark.indexer.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.*;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyH2Utils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReader;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReaderFake;

import com.fasterxml.jackson.databind.ObjectMapper;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TaxonomyDocumentsToHDFSWriterTest {

    private JobParameter parameter;
    private Connection dbConnection;

    @BeforeAll
    void setUpWriter() throws SQLException, IOException {
        ResourceBundle application = SparkUtils.loadApplicationProperty("application-taxonomy");
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();

        // Taxonomy H2 database create/load database data
        String url = application.getString("database.url");
        String user = application.getString("database.user.name");
        String password = application.getString("database.password");
        dbConnection = DriverManager.getConnection(url, user, password);
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.createTables(statement);
        TaxonomyH2Utils.insertData(statement);
    }

    @AfterAll
    void closeWriter() throws SQLException, IOException {
        parameter.getSparkContext().close();
        // Taxonomy H2 database clean
        Statement statement = this.dbConnection.createStatement();
        TaxonomyH2Utils.dropTables(statement);
        dbConnection.close();
    }

    @Test
    void writeIndexDocumentsToHDFS() throws Exception {
        TaxonomyDocumentsToHDFSWriterFake writer = new TaxonomyDocumentsToHDFSWriterFake(parameter);
        writer.writeIndexDocumentsToHDFS();
        Map<String, TaxonomyDocument> documents =
                writer.getSavedDocuments().stream()
                        .collect(Collectors.toMap(TaxonomyDocument::getId, Function.identity()));
        assertNotNull(documents);

        validateDeleted(documents.get("500"));
        validateMerged(documents.get("50"));
        validateActive(documents.get("10116"));
        validateLineageTaxonomyWithoutProtein(documents.get("10114"));
    }

    private void validateLineageTaxonomyWithoutProtein(TaxonomyDocument document) {
        assertNotNull(document);

        assertNotNull(document.getTaxonomiesWith());
        assertTrue(document.getTaxonomiesWith().isEmpty());

        assertNotNull(document.getTaxonomyObj());
        TaxonomyEntry entry = getEntry(document.getTaxonomyObj());
        assertNotNull(entry);
        assertNotNull(entry.getStatistics());
        TaxonomyStatistics stat = entry.getStatistics();
        assertEquals(2, stat.getReviewedProteinCount());
        assertEquals(2, stat.getUnreviewedProteinCount());
        assertEquals(0, stat.getProteomeCount());
        assertEquals(0, stat.getReferenceProteomeCount());
    }

    private void validateActive(TaxonomyDocument document) {
        assertNotNull(document);
        assertNotNull(document.getTaxonomyObj());
        assertEquals("10116", document.getId());
        assertEquals(10116L, document.getTaxId());
        assertEquals(10114L, document.getParent());
        assertEquals(TaxonomyRank.SPECIES.name(), document.getRank().toUpperCase(Locale.ROOT));
        assertEquals("Rattus norvegicus", document.getScientific());
        assertEquals("Rat", document.getCommon());
        assertEquals("RAT", document.getMnemonic());

        assertNotNull(document.getOtherNames());
        assertTrue(document.getOtherNames().contains("first name"));
        assertTrue(document.getOtherNames().contains("second name"));

        assertNotNull(document.getTaxonomiesWith());
        assertEquals(5, document.getTaxonomiesWith().size());
        assertTrue(document.getTaxonomiesWith().contains("1_uniprotkb"));
        assertTrue(document.getTaxonomiesWith().contains("2_reviewed"));
        assertTrue(document.getTaxonomiesWith().contains("3_unreviewed"));
        assertTrue(document.getTaxonomiesWith().contains("4_reference"));
        assertTrue(document.getTaxonomiesWith().contains("5_proteome"));

        TaxonomyEntry entry = getEntry(document.getTaxonomyObj());
        assertNotNull(entry);

        assertEquals(10116L, entry.getTaxonId());
        assertEquals("RAT", entry.getMnemonic());
        assertEquals("Rattus norvegicus", entry.getScientificName());
        assertEquals("Rat", entry.getCommonName());

        assertNotNull(entry.getParent());
        assertEquals(10114L, entry.getParent().getTaxonId());
        assertEquals("scientificName for 10114", entry.getParent().getScientificName());
        assertEquals("commonName for 10114", entry.getParent().getCommonName());

        assertEquals(TaxonomyRank.SPECIES, entry.getRank());
        assertTrue(entry.isActive());
        assertTrue(entry.isHidden());
        assertTrue(entry.getOtherNames().contains("first name"));
        assertTrue(entry.getOtherNames().contains("second name"));

        Map<Long, TaxonomyLineage> lineageMap =
                entry.getLineages().stream()
                        .collect(
                                Collectors.toMap(TaxonomyLineage::getTaxonId, Function.identity()));
        assertEquals(3, lineageMap.size());
        assertTrue(lineageMap.containsKey(10114L));
        assertEquals("scientificName for 10114", lineageMap.get(10114L).getScientificName());
        assertTrue(lineageMap.containsKey(39107L));
        assertEquals("scientificName for 39107", lineageMap.get(39107L).getScientificName());
        assertTrue(lineageMap.containsKey(10066L));
        assertEquals("scientificName for 10066", lineageMap.get(10066L).getScientificName());

        assertNotNull(entry.getStrains());
        assertEquals(2, entry.getStrains().size());

        assertEquals("strain 1", entry.getStrains().get(0).getName());
        assertTrue(entry.getStrains().get(0).hasSynonyms());
        assertEquals("strain 2", entry.getStrains().get(1).getName());
        assertTrue(entry.getStrains().get(1).hasSynonyms());

        assertNotNull(entry.getHosts());
        assertEquals(2, entry.getHosts().size());
        assertEquals(337687L, entry.getHosts().get(0).getTaxonId());
        assertEquals(289376L, entry.getHosts().get(1).getTaxonId());

        assertNotNull(entry.getLinks());
        assertEquals(2, entry.getLinks().size());
        assertTrue(entry.getLinks().contains("uri 3"));
        assertTrue(entry.getLinks().contains("uri 4"));

        assertNotNull(entry.getStatistics());
        TaxonomyStatistics stat = entry.getStatistics();
        assertEquals(2, stat.getReviewedProteinCount());
        assertEquals(2, stat.getUnreviewedProteinCount());
        assertEquals(2, stat.getProteomeCount());
        assertEquals(1, stat.getReferenceProteomeCount());

        assertNull(entry.getInactiveReason());
    }

    private void validateMerged(TaxonomyDocument document) {
        assertNotNull(document.getTaxonomyObj());
        assertEquals("50", document.getId());
        assertFalse(document.isActive());
        TaxonomyEntry entry = getEntry(document.getTaxonomyObj());
        assertEquals(50L, entry.getTaxonId());
        assertNotNull(entry.getInactiveReason());
        TaxonomyInactiveReason reason = entry.getInactiveReason();
        assertEquals(TaxonomyInactiveReasonType.MERGED, reason.getInactiveReasonType());
        assertEquals(10116L, reason.getMergedTo());
    }

    private void validateDeleted(TaxonomyDocument document) {
        assertNotNull(document.getTaxonomyObj());
        assertEquals("500", document.getId());
        assertFalse(document.isActive());
        TaxonomyEntry entry = getEntry(document.getTaxonomyObj());
        assertEquals(500L, entry.getTaxonId());
        assertNotNull(entry.getInactiveReason());
        TaxonomyInactiveReason reason = entry.getInactiveReason();
        assertEquals(TaxonomyInactiveReasonType.DELETED, reason.getInactiveReasonType());
        assertEquals(0L, reason.getMergedTo());
    }

    private TaxonomyEntry getEntry(byte[] bytes) {
        try {
            ObjectMapper objectMapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();
            return objectMapper.readValue(bytes, TaxonomyEntry.class);
        } catch (IOException e) {
            throw new IndexDataStoreException("Unable to parse taxonomy", e);
        }
    }

    private static class TaxonomyDocumentsToHDFSWriterFake extends TaxonomyDocumentsToHDFSWriter {

        private List<TaxonomyDocument> documents;
        private final JobParameter parameter;

        public TaxonomyDocumentsToHDFSWriterFake(JobParameter parameter) {
            super(parameter);
            this.parameter = parameter;
        }

        @Override
        TaxonomyRDDReader getTaxonomyRDDReader() {
            return new TaxonomyRDDReaderFake(parameter, true, false);
        }

        @Override
        void saveToHDFS(JavaRDD<TaxonomyDocument> taxonomyDocumentRDD) {
            documents = taxonomyDocumentRDD.collect();
        }

        List<TaxonomyDocument> getSavedDocuments() {
            return documents;
        }
    }
}
