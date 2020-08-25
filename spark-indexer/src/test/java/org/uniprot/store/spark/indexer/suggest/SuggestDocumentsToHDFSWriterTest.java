package org.uniprot.store.spark.indexer.suggest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.search.document.suggest.SuggestDictionary.*;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.common.JobParameter;
import org.uniprot.store.spark.indexer.common.util.SparkUtils;
import org.uniprot.store.spark.indexer.uniprot.UniProtKBRDDTupleReader;

/**
 * @author lgonzales
 * @since 17/05/2020
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SuggestDocumentsToHDFSWriterTest {

    private JobParameter parameter;
    private JavaRDD<String> flatFileRDD;

    @BeforeAll
    void setUpWriter() {
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
    }

    @AfterAll
    void closeWriter() {
        parameter.getSparkContext().close();
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
        assertEquals(2L, count);
        SuggestDocument document = suggestRdd.first();

        assertNotNull(document);
        assertEquals(CATALYTIC_ACTIVITY.name(), document.dictionary);
        assertEquals("CHEBI:16526", document.id);
        assertEquals("carbon dioxide", document.value);
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
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> suggestRdd = writer.getProteome();
        assertNotNull(suggestRdd);
        int count = (int) suggestRdd.count();
        assertEquals(6, count);

        Map<String, List<SuggestDocument>> resultMap =
                getResultMap(suggestRdd.take(count), doc -> doc.id);

        assertThat(resultMap.containsKey("UP000008687"), is(true));
        assertThat(resultMap.get("UP000008687").size(), is(1));
        assertNotNull(resultMap.get("UP000008687").get(0));
        assertEquals(PROTEOME_UPID.name(), resultMap.get("UP000008687").get(0).dictionary);
        assertEquals("UP000008687", resultMap.get("UP000008687").get(0).id);
        assertEquals("Potato virus X (strain X3) (PVX)", resultMap.get("UP000008687").get(0).value);
        assertEquals(1, resultMap.get("UP000008687").get(0).altValues.size());
        assertEquals("UP000008687", resultMap.get("UP000008687").get(0).altValues.get(0));
    }

    @Disabled
    @Test
    void getUniParcTaxonomy() {
        // FIXME: 25/08/2020 Update lineage query to be supported by H2 or pass another equivalent
        // query
        SuggestDocumentsToHDFSWriter writer = new SuggestDocumentsToHDFSWriter(parameter);
        JavaRDD<SuggestDocument> taxonomyRDD = writer.getUniParcTaxonomy();
        assertNotNull(taxonomyRDD);
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
