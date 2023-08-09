package org.uniprot.store.spark.indexer.suggest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.search.document.suggest.SuggestDictionary.*;
import static org.uniprot.store.spark.indexer.common.util.CommonVariables.SPARK_LOCAL_MASTER;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyH2Utils;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyRDDReaderFake;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

import com.typesafe.config.Config;

/**
 * @author lgonzales
 * @since 17/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SuggestDocumentsToHPSWriterTest {

    private JobParameter parameter;
    private JavaRDD<String> flatFileRDD;
    private Connection dbConnection;

    @BeforeAll
    void setUpWriter() throws SQLException, IOException {
        Config application = SparkUtils.loadApplicationProperty();
        JavaSparkContext sparkContext =
                SparkUtils.loadSparkContext(application, SPARK_LOCAL_MASTER);
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
    void testWriteIndexDocumentsToHPS(@TempDir Path hpsPath) {
        SuggestDocumentsToHPSWriter writer = Mockito.mock(SuggestDocumentsToHPSWriter.class);
        Mockito.doCallRealMethod()
                .when(writer)
                .writeIndexDocumentsToHPS(Mockito.anyInt(), Mockito.anyString());
        JavaRDD<SuggestDocument> emptyRDD =
                parameter.getSparkContext().parallelize(new ArrayList<>());

        Mockito.when(writer.getMain()).thenReturn(emptyRDD);
        Mockito.verify(writer, Mockito.atMostOnce()).getMain();
        Mockito.when(writer.getKeyword()).thenReturn(emptyRDD);
        Mockito.when(writer.getSubcell()).thenReturn(emptyRDD);
        Mockito.when(writer.getEC(Mockito.any())).thenReturn(emptyRDD);
        Mockito.when(writer.getChebi(Mockito.any())).thenReturn(emptyRDD);
        Mockito.when(writer.getRheaComp(Mockito.any())).thenReturn(emptyRDD);
        Mockito.when(writer.getGo(Mockito.any())).thenReturn(emptyRDD);
        Mockito.when(writer.getUniprotKbOrganism(Mockito.any(), Mockito.any()))
                .thenReturn(emptyRDD);
        Mockito.when(writer.getProteome(Mockito.any())).thenReturn(emptyRDD);
        Mockito.when(writer.getUniParcTaxonomy(Mockito.any())).thenReturn(emptyRDD);

        writer.writeIndexDocumentsToHPS(
                1, hpsPath.toString() + File.separator + "testWriteIndexDocumentsToHPS");

        Mockito.verify(writer, Mockito.atMostOnce()).getMain();
        Mockito.verify(writer, Mockito.atMostOnce()).getKeyword();
        Mockito.verify(writer, Mockito.atMostOnce()).getSubcell();
        Mockito.verify(writer, Mockito.atMostOnce()).getEC(Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce()).getChebi(Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce()).getRheaComp(Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce()).getGo(Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce())
                .getUniprotKbOrganism(Mockito.any(), Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce()).getProteome(Mockito.any());
        Mockito.verify(writer, Mockito.atMostOnce()).getUniParcTaxonomy(Mockito.any());
    }

    @Test
    void testGetFlatFileRDD() {
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
        JavaRDD<String> result = writer.getFlatFileRDD();
        assertNotNull(result);
        assertEquals(1, result.count());
    }

    @Test
    void getMain() {
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getChebi(flatFileRDD);
        assertNotNull(suggestRdd);
        long count = suggestRdd.count();
        assertEquals(36L, count);
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
        assertEquals(15, chebiDocs.size());
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

        List<SuggestDocument> bindingDocs =
                suggestRdd.filter(c -> c.dictionary.equals(BINDING.name())).collect();
        assertEquals(4, bindingDocs.size());
        document =
                bindingDocs.stream()
                        .filter(c -> c.id.equals("CHEBI:6700"))
                        .findFirst()
                        .orElseThrow(AssertionFailedError::new);

        assertEquals(BINDING.name(), document.dictionary);
        assertEquals("CHEBI:6700", document.id);
        assertEquals("6700-conjugate-acid-relation-chain", document.value);
        assertEquals("medium", document.importance);

        assertEquals(0, document.altValues.size());

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

        List<String> bindingIds =
                catalyticDocs.stream()
                        .map(SuggestDocument::getDocumentId)
                        .collect(Collectors.toList());

        assertTrue(chebiIds.containsAll(cofactorIds));
        assertTrue(chebiIds.containsAll(catalyticIds));
        assertTrue(chebiIds.containsAll(bindingIds));
    }

    @Test
    void getRheaComp() {
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);
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
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);

        JavaRDD<SuggestDocument> suggestRdd =
                writer.getProteome(
                        new TaxonomyRDDReaderFake(parameter, true, true).loadTaxonomyLineage());
        assertNotNull(suggestRdd);
        var suggests = suggestRdd.collect();

        var totalEntriesInXmlFile = 7;
        var totalNumbersOfDefaultTaxonSynonyms = 28;
        var upidTaxonomyDocsCount = 7;
        var duplicateTaxonIdInXmlFile = 1;
        var alreadyPresentInSynonymsFile = 1;
        var extraLineageFromTaxonomyRDDReaderFake = 7;
        var organismDocsCount =
                totalNumbersOfDefaultTaxonSynonyms
                        + totalEntriesInXmlFile
                        - duplicateTaxonIdInXmlFile
                        - alreadyPresentInSynonymsFile;
        var taxonomyDocsCount = organismDocsCount + extraLineageFromTaxonomyRDDReaderFake;
        assertEquals(
                upidTaxonomyDocsCount + organismDocsCount + taxonomyDocsCount, suggests.size());

        var resultMap = getResultMap(suggests.subList(0, upidTaxonomyDocsCount), doc -> doc.id);
        assertOrganismNameToUpIdSuggest(resultMap);

        resultMap =
                getResultMap(
                        suggests.subList(
                                upidTaxonomyDocsCount, upidTaxonomyDocsCount + organismDocsCount),
                        doc -> doc.id);
        assertNameToIdForProteomeSuggest(resultMap, PROTEOME_ORGANISM);
        assertTaxons(resultMap, false);

        resultMap =
                getResultMap(
                        suggests.subList(
                                upidTaxonomyDocsCount + organismDocsCount, suggests.size()),
                        doc -> doc.id);
        assertNameToIdForProteomeSuggest(resultMap, PROTEOME_TAXONOMY);
        assertTaxons(resultMap, true);
    }

    @Test
    void getUniprotKbOrganism() {
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);

        JavaRDD<SuggestDocument> suggestRdd =
                writer.getUniprotKbOrganism(
                        flatFileRDD,
                        new TaxonomyRDDReaderFake(parameter, true, true).loadTaxonomyLineage());
        assertNotNull(suggestRdd);
        var suggests = suggestRdd.collect();

        var totalEntriesInXmlFile = 1;
        var totalNumbersOfDefaultTaxonSynonyms = 35;
        var totalHostEntriesInXmlFile = 0;
        var totalNumbersOfDefaultHostSynonyms = 18;
        var totalHostSynonymsDocs = totalNumbersOfDefaultHostSynonyms + totalHostEntriesInXmlFile;
        var alreadyPresentInSynonymsFile = 1;
        var extraLineageFromTaxonomyRDDReaderFake = 3;
        var organismDocsCount =
                totalNumbersOfDefaultTaxonSynonyms
                        + totalEntriesInXmlFile
                        - alreadyPresentInSynonymsFile;
        var taxonomyDocsCount = organismDocsCount + extraLineageFromTaxonomyRDDReaderFake;
        assertEquals(
                totalHostSynonymsDocs + organismDocsCount + taxonomyDocsCount, suggests.size());

        var resultMap = getResultMap(suggests.subList(0, organismDocsCount), doc -> doc.id);
        assertTaxons(resultMap, false);

        resultMap =
                getResultMap(
                        suggests.subList(taxonomyDocsCount + organismDocsCount, suggests.size()),
                        doc -> doc.id);
        assertTaxons(resultMap, false);
    }

    @Test
    void getUniParcTaxonomy() {
        SuggestDocumentsToHPSWriter writer = new SuggestDocumentsToHPSWriter(parameter);

        JavaRDD<SuggestDocument> suggestRdd =
                writer.getUniParcTaxonomy(
                        new TaxonomyRDDReaderFake(parameter, true, true).loadTaxonomyLineage());
        assertNotNull(suggestRdd);
        var suggests = suggestRdd.collect();

        var totalEntriesInXmlFile = 2;
        var totalNumbersOfDefaultTaxonSynonyms = 35;
        var alreadyPresentInSynonymsFile = 1;
        var extraLineageFromTaxonomyRDDReaderFake = 3;
        var organismDocsCount =
                totalNumbersOfDefaultTaxonSynonyms
                        + totalEntriesInXmlFile
                        - alreadyPresentInSynonymsFile;
        var taxonomyDocsCount = organismDocsCount + extraLineageFromTaxonomyRDDReaderFake;
        assertEquals(taxonomyDocsCount, suggests.size());

        var resultMap = getResultMap(suggests, doc -> doc.id);
        assertAll(
                () -> assertTrue(resultMap.containsKey("10114")),
                () -> assertTrue(resultMap.containsKey("39107")),
                () -> assertTrue(resultMap.containsKey("10066")));
    }

    @Nested
    class GetDefaultHighImportantTaxonTest {
        private final SuggestDocumentsToHPSWriter writer =
                new SuggestDocumentsToHPSWriter(parameter);

        @ParameterizedTest
        @EnumSource(
                value = SuggestDictionary.class,
                names = {"UNIPARC_TAXONOMY", "TAXONOMY", "ORGANISM"},
                mode = EnumSource.Mode.INCLUDE)
        void canLoadDefaultHighImportantTaxonomy(SuggestDictionary dict) {
            var dicRDD = writer.getDefaultHighImportantTaxon(dict);
            assertEquals(35, dicRDD.count());
        }

        @Test
        void canLoadDefaultHighImportantHost() {
            var dicRDD = writer.getDefaultHighImportantTaxon(HOST);
            assertEquals(18, dicRDD.count());
        }

        @ParameterizedTest
        @EnumSource(
                value = SuggestDictionary.class,
                names = {"PROTEOME_TAXONOMY", "PROTEOME_ORGANISM"},
                mode = EnumSource.Mode.INCLUDE)
        void canLoadDefaultHighImportantProteome(SuggestDictionary dict) {
            var dicRDD = writer.getDefaultHighImportantTaxon(dict);
            assertEquals(28, dicRDD.count());
        }
    }

    private void assertTaxons(Map<String, List<SuggestDocument>> resultMap, boolean isTaxonomy) {
        assertAll(
                () -> assertEquals(resultMap.containsKey("10114"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("39107"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("10066"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("289375"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("60713"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("1076254"), isTaxonomy),
                () -> assertEquals(resultMap.containsKey("1559364"), isTaxonomy));
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

    private void assertNameToIdForProteomeSuggest(
            Map<String, List<SuggestDocument>> resultMap, SuggestDictionary dict) {
        final var taxonId = "1559365";
        assertTrue(resultMap.containsKey(taxonId));
        assertEquals(1, resultMap.get(taxonId).size());
        assertNotNull(resultMap.get(taxonId).get(0));
        assertEquals(dict.name(), resultMap.get(taxonId).get(0).dictionary);
        assertEquals(taxonId, resultMap.get(taxonId).get(0).id);
        assertEquals("scientificName for " + taxonId, resultMap.get(taxonId).get(0).value);
        assertEquals(1, resultMap.get(taxonId).get(0).altValues.size());
        assertEquals("commonName for " + taxonId, resultMap.get(taxonId).get(0).altValues.get(0));
    }

    private <T> Map<String, List<T>> getResultMap(
            List<T> result, Function<T, String> mappingFunction) {
        Map<String, List<T>> map = result.stream().collect(Collectors.groupingBy(mappingFunction));
        for (Map.Entry<String, List<T>> stringListEntry : map.entrySet()) {
            assertThat(stringListEntry.getValue(), hasSize(1));
        }
        return map;
    }
}
