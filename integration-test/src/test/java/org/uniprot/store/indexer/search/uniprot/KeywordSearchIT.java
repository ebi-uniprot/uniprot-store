package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;

/** Verifies if the protein keywords are indexed correctly */
class KeywordSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String CHEMICAL = "2Fe-2S";
    private static final String ARCHAEAL_FLAGELLUM = "Archaeal flagellum";
    private static final String ARCHAEAL_FLAGELLUM_BIOGENESIS = "Archaeal flagellum biogenesis";
    private static final String APOPLAST = "Apoplast";
    private static final String AMYLOID = "Amyloid";
    private static final String AMYLOIDOSIS = "Amyloidosis";
    private static final String ACCESSION1 = "Q197F4";
    private static final String ACCESSION2 = "Q197F5";
    private static final String ACCESSION3 = "Q197F6";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.KW, createKWLine(CHEMICAL, ARCHAEAL_FLAGELLUM));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(
                LineType.KW, createKWLine(ARCHAEAL_FLAGELLUM_BIOGENESIS, AMYLOID));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.KW, createKWLine(APOPLAST, AMYLOIDOSIS));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createKWLine(String... keywords) {
        return TestUtils.createMultiElementFFLine("KW   ", ";", ".", keywords);
    }

    @Test
    void noMatchesForSubStringKeyword() {
        String keywordSubString = CHEMICAL.substring(0, CHEMICAL.length() - 2);
        String query = keyword(keywordSubString);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void amyloidSearchDoesNotMatchEntryWithAmyloidosis() {
        String query = keyword(AMYLOID);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, not(hasItem(ACCESSION3)));
    }

    @Test
    void apoplastKeywordMatchesEntry3() {
        String query = keyword(APOPLAST);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void lowerCaseApoplastMatchesEntry3() {
        String query = keyword(APOPLAST.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void upperCaseApoplastMatchesEntry3() {
        String query = keyword(APOPLAST.toUpperCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void archaealFlagellumKeywordMatchesEntries1And2() {
        String query = keyword(ARCHAEAL_FLAGELLUM);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1, ACCESSION2));
    }

    @Test
    void flagellumArchaealKeywordMatchesEntry1And2() {
        String query = keyword("flagellum Archaeal");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2));
    }

    @Test
    void archaealBiogenesisKeywordMatchesEntry2() {
        String query = keyword("Archaeal biogenesis");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void archaealFlagellumBiogenesisKeywordMatchesEntry2() {
        String query = keyword(ARCHAEAL_FLAGELLUM_BIOGENESIS);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    String keyword(String value) {
        return query(UniProtKBSearchFields.INSTANCE.getField("keyword"), value);
    }
}
