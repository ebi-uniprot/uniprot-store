package org.uniprot.store.spark.indexer.suggest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doReturn;
import static org.uniprot.store.search.document.suggest.SuggestDictionary.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
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
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyH2Utils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReaderFake;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

/**
 * @author lgonzales
 * @since 17/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SuggestDocumentsToHDFSWriterTest {

    private JobParameter parameter;
    private JavaRDD<String> flatFileRDD;
    private Connection dbConnection;

    @BeforeAll
    void setUpWriter() throws SQLException, IOException {
        ResourceBundle application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext = SparkUtils.loadSparkContext(application);
        parameter =
                JobParameter.builder()
                        .applicationConfig(application)
                        .releaseName("2020_02")
                        .sparkContext(sparkContext)
                        .build();
        UniProtKBRDDTupleReader reader = new UniProtKBRDDTupleReader(parameter, false);
        flatFileRDD = reader.loadFlatFileToRDD();

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
    void getMain() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getMain();
        assertNotNull(suggestRdd);
        long count = suggestRdd.count();
        assertTrue(count > 150);
        SuggestDocument document = suggestRdd.first();

        assertNotNull(document);
        assertEquals(MAIN.name(), document.dictionary);
        assertEquals("Database: EMBL", document.value);
    }

    @Test
    void getGo() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getGo(flatFileRDD);
        assertNotNull(suggestRdd);
        long count = suggestRdd.count();
        assertEquals(8L, count);
        SuggestDocument document = suggestRdd.first();

        assertNotNull(document);
        assertEquals(GO.name(), document.dictionary);
        assertEquals("0005719", document.id);
        assertEquals("nuclear euchromatin", document.value);
        assertTrue(document.altValues.contains("GO:0005719"));
    }

    @Test
    void getChebi() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getChebi(flatFileRDD);
        assertNotNull(suggestRdd);
        long count = suggestRdd.count();
        assertEquals(30L, count);
        List<SuggestDocument> catalyticDocs =
                suggestRdd.filter(c -> c.dictionary.equals(CATALYTIC_ACTIVITY.name())).collect();
        assertEquals(10, catalyticDocs.size());
        assertNotNull(catalyticDocs);
        SuggestDocument document =
                catalyticDocs.stream()
                        .filter(c -> c.id.equals("CHEBI:23367"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals(CATALYTIC_ACTIVITY.name(), document.dictionary);
        assertEquals("CHEBI:23367", document.id);
        assertEquals("molecular entity", document.value);
        assertEquals("medium", document.importance);

        assertEquals(5, document.altValues.size());
        assertTrue(document.altValues.contains("entidad molecular"));
        assertTrue(document.altValues.contains("entidades moleculares"));
        assertTrue(document.altValues.contains("entite moleculaire"));
        assertTrue(document.altValues.contains("molecular entities"));
        assertTrue(document.altValues.contains("molekulare Entitaet"));

        // Make sure we add relatedIds to suggest as well
        List<SuggestDocument> cofactorDocs =
                suggestRdd.filter(c -> c.dictionary.equals(COFACTOR.name())).collect();
        assertEquals(7, cofactorDocs.size());
        assertNotNull(cofactorDocs);
        document =
                cofactorDocs.stream()
                        .filter(c -> c.id.equals("CHEBI:2500"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals(COFACTOR.name(), document.dictionary);
        assertEquals("CHEBI:2500", document.id);
        assertEquals("2500-fluoroethyl methanesulfonate", document.value);
        assertEquals("medium", document.importance);

        assertEquals(2, document.altValues.size());
        assertTrue(document.altValues.contains("2500-synonym"));
        assertTrue(document.altValues.contains("AABBBCCCDD-IIHHHHGGGFFFF-N"));

        List<SuggestDocument> chebiDocs =
                suggestRdd.filter(c -> c.dictionary.equals(CHEBI.name())).collect();
        assertNotNull(chebiDocs);
        document =
                chebiDocs.stream()
                        .filter(c -> c.id.equals("CHEBI:2500"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals(CHEBI.name(), document.dictionary);
        assertEquals("CHEBI:2500", document.id);
        assertEquals("2500-fluoroethyl methanesulfonate", document.value);
        assertEquals("medium", document.importance);

        assertEquals(2, document.altValues.size());
        assertTrue(document.altValues.contains("2500-synonym"));
        assertTrue(document.altValues.contains("AABBBCCCDD-IIHHHHGGGFFFF-N"));

        assertEquals(13, chebiDocs.size());
        List<String> chebiIds =
                chebiDocs.stream().map(SuggestDocument::getDocumentId).collect(Collectors.toList());
        List<String> cofactorIds =
                cofactorDocs.stream()
                        .map(SuggestDocument::getDocumentId)
                        .collect(Collectors.toList());
        List<String> catalyticIds =
                catalyticDocs.stream()
                        .map(SuggestDocument::getDocumentId)
                        .collect(Collectors.toList());

        assertTrue(chebiIds.containsAll(cofactorIds));
        assertTrue(chebiIds.containsAll(catalyticIds));
    }

    @Test
    void getRheaComp() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> rheaCompRdd = writer.getRheaComp(flatFileRDD);
        assertNotNull(rheaCompRdd);
        long count = rheaCompRdd.count();
        assertEquals(2L, count);
        SuggestDocument document = rheaCompRdd.first();

        assertNotNull(document);
        assertEquals(CATALYTIC_ACTIVITY.name(), document.dictionary);
        assertEquals("RHEA-COMP:10694", document.id);
        assertEquals("N(4)-acetylcytidine(34) in elongator tRNA(Met)", document.value);
        assertNotNull(document.altValues);
        assertTrue(document.altValues.isEmpty());
    }

    @Test
    void getEC() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getEC(flatFileRDD);
        assertNotNull(suggestRdd);
        long count = suggestRdd.count();
        assertEquals(1L, count);
        SuggestDocument document = suggestRdd.first();

        assertNotNull(document);
        assertEquals(EC.name(), document.dictionary);
        assertEquals("2.7.10.2", document.id);
        assertEquals("Non-specific protein-tyrosine kinase", document.value);
    }

    @Test
    void getSubcell() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getSubcell();
        assertNotNull(suggestRdd);
        int count = (int) suggestRdd.count();
        assertEquals(520, count);

        Map<String, List<SuggestDocument>> resultMap =
                getResultMap(suggestRdd.take(count), doc -> doc.id);

        assertThat(resultMap.containsKey("SL-0187"), is(true));
        assertNotNull(resultMap.get("SL-0187").get(0));
        assertEquals(SUBCELL.name(), resultMap.get("SL-0187").get(0).dictionary);
        assertEquals("SL-0187", resultMap.get("SL-0187").get(0).id);
        assertEquals("Nucleoid", resultMap.get("SL-0187").get(0).value);
    }

    @Test
    void getKeyword() {
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getKeyword();
        assertNotNull(suggestRdd);
        int count = (int) suggestRdd.count();
        assertEquals(8, count);

        Map<String, List<SuggestDocument>> resultMap =
                getResultMap(suggestRdd.take(count), doc -> doc.id);

        assertThat(resultMap.containsKey("KW-9997"), is(true));
        assertNotNull(resultMap.get("KW-9997").get(0));
        assertEquals(KEYWORD.name(), resultMap.get("KW-9997").get(0).dictionary);
        assertEquals("KW-9997", resultMap.get("KW-9997").get(0).id);
        assertEquals("Coding sequence diversity", resultMap.get("KW-9997").get(0).value);
    }

    @Test
    void getProteome() {
        SuggestDocumentsToHDFSWriter writer =
                Mockito.spy(new SuggestDocumentsToHDFSWriter(parameter));
        doReturn(new TaxonomyRDDReaderFake(parameter, true).loadTaxonomyLineage())
                .when(writer)
                .getOrganismWithLineageRDD();

        JavaRDD<SuggestDocument> suggestRdd = writer.getProteome();
        assertNotNull(suggestRdd);
        var suggests = suggestRdd.collect();
        int count = (int) suggestRdd.count();

        var upidTaxonomyDocsCount = 7;
        var organismDocsCount = 7;
        var taxonomyDocsCount = 7;
        assertEquals(
                upidTaxonomyDocsCount + organismDocsCount + taxonomyDocsCount, suggests.size());

        var resultMap = getResultMap(suggests.subList(0, upidTaxonomyDocsCount), doc -> doc.id);
        assertOrganismNameToUpIdSuggest(resultMap);

        resultMap =
                getResultMap(
                        suggests.subList(
                                upidTaxonomyDocsCount, upidTaxonomyDocsCount + organismDocsCount),
                        doc -> doc.id);
        assertNotNull(resultMap);

        resultMap =
                getResultMap(
                        suggests.subList(
                                upidTaxonomyDocsCount + organismDocsCount, suggests.size()),
                        doc -> doc.id);
        assertNotNull(resultMap);
    }

    private void assertOrganismNameToUpIdSuggest(Map<String, List<SuggestDocument>> resultMap) {
        assertTrue(resultMap.containsKey("UP000006687"));
        assertEquals(1, resultMap.get("UP000006687").size());
        assertNotNull(resultMap.get("UP000006687").get(0));
        assertEquals(PROTEOME_UPID.name(), resultMap.get("UP000006687").get(0).dictionary);
        assertEquals("UP000006687", resultMap.get("UP000006687").get(0).id);
        assertEquals("scientificName for 11049", resultMap.get("UP000006687").get(0).value);
        assertEquals(1, resultMap.get("UP000006687").get(0).altValues.size());
        assertEquals("UP000006687", resultMap.get("UP000006687").get(0).altValues.get(0));
    }

    private <T> Map<String, List<T>> getResultMap(
            List<T> result, Function<T, String> mappingFunction) {
        Map<String, List<T>> map = result.stream().collect(Collectors.groupingBy(mappingFunction));
        for (Map.Entry<String, List<T>> stringListEntry : map.entrySet()) {
            //            assertThat(stringListEntry.getValue(), hasSize(1));
        }
        return map;
    }
}
