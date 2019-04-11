package uk.ac.ebi.uniprot.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.indexer.document.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

import static uk.ac.ebi.uniprot.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.*;

/**
 * Verifies if the protein keywords are indexed correctly
 */
public class KeywordSearchIT {
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
    @ClassRule
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.KW, createKWLine(CHEMICAL, ARCHAEAL_FLAGELLUM));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.KW, createKWLine(ARCHAEAL_FLAGELLUM_BIOGENESIS, AMYLOID));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.KW, createKWLine(APOPLAST, AMYLOIDOSIS));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createKWLine(String... keywords) {
        return TestUtils.createMultiElementFFLine("KW   ", ";", ".", keywords);
    }

    @Test
    public void noMatchesForSubStringKeyword() throws Exception {
        String keywordSubString = CHEMICAL.substring(0, CHEMICAL.length() - 2);
        String query = keyword(keywordSubString);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void amyloidSearchDoesNotMatchEntryWithAmyloidosis() throws Exception {
        String query = keyword(AMYLOID);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, not(hasItem(ACCESSION3)));
    }

    @Test
    public void apoplastKeywordMatchesEntry3() throws Exception {
        String query = keyword(APOPLAST);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void lowerCaseApoplastMatchesEntry3() throws Exception {
        String query = keyword(APOPLAST.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void upperCaseApoplastMatchesEntry3() throws Exception {
        String query = keyword(APOPLAST.toUpperCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void archaealFlagellumKeywordMatchesEntries1And2() throws Exception {
        String query = keyword(ARCHAEAL_FLAGELLUM);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1, ACCESSION2));
    }

    @Test
    public void flagellumArchaealKeywordMatchesEntry1And2() throws Exception {
        String query = keyword("flagellum Archaeal");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2));
    }

    @Test
    public void archaealBiogenesisKeywordMatchesEntry2() throws Exception {
        String query = keyword("Archaeal biogenesis");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void archaealFlagellumBiogenesisKeywordMatchesEntry2() throws Exception {
        String query = keyword(ARCHAEAL_FLAGELLUM_BIOGENESIS);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }
    String keyword(String value) {
    	return query(UniProtField.Search.keyword, value);
    }
}