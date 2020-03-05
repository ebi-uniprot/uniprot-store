package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
import org.uniprot.store.search.field.QueryBuilder;

/** Test the behaviour of searching GO terms */
class GoTermSearchIT {
    private static final String GO_1 = "T1TTT2";
    private static final String GO_2 = "T1TTT3";
    private static final String GO_3 = "T1TTT4";
    private static final String GO_4 = "T1TTT5";
    private static final String GO_5 = "T1TTT6";
    private static final String GO_6 = "T1TTT7";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // GO refs
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_1));
        entryProxy.updateEntryObject(
                LineType.DR, "DR   GO; GO:0033644; C:host cell membrane; IEA:UniProtKB-KW.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_2));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0033645; C:host wheresTheSundayTimes membrane; IEA:UniProtKB-KW.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_3));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0000175; F:3'-5'-exoribonuclease activity; IBA:GO_Central.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_4));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0000009; F:alpha-1,6-mannosyltransferase activity; ISS:UniProtKB.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_5));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0001874; F:(1->3)-beta-D-glucan receptor activity; IDA:MGI.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_6));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0001874; F:(1->4)-beta-D-marsBars receptor activity; IDA:MGI.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void goEvidenceSingle() {
        String query = goTerm("iba", "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_3));
        // assertThat(retrievedAccessions, containsInAnyOrder(GO_5, GO_6));
    }

    @Test
    void goEvidenceTwo() {
        String query = goTerm("ida", "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);

        assertThat(retrievedAccessions, containsInAnyOrder(GO_5, GO_6));
    }

    @Test
    void goWithEvidence() {
        String query = goTerm("iea", "0033644");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(GO_1, GO_2));
    }

    @Test
    void goWithEvidence2() {
        String query = goTerm("iea", "0033645");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(GO_2));
    }

    @Test
    void goWithEvidenceTwo() {
        String query = goTerm("ida", "0001874");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);

        assertThat(retrievedAccessions, containsInAnyOrder(GO_5, GO_6));
    }

    @Test
    void goExactlyCorrectTerms() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_1);
        query = QueryBuilder.and(query, goTerm("host cell membrane"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_1));
    }

    @Test
    void goPartialTermListMiddleWordMissing() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_1);
        query = QueryBuilder.and(QueryBuilder.and(query, goTerm("host")), goTerm("membrane"));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_1));
    }

    @Test
    void goPartialTermList() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_1);
        query = QueryBuilder.and(query, goTerm("cell membrane"));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_1));
    }

    @Test
    void goExactTermsWhichUse5And3Prime() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_3);
        query = QueryBuilder.and(query, goTerm("3'-5'-exoribonuclease activity"));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_3));
    }

    @Test
    void goPartialWith5And3Prime() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_3);
        query = QueryBuilder.and(query, goTerm("exoribonuclease"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_3));
    }

    @Test
    void goExact1Comma6() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("accession"),
                        GO_4);
        query = QueryBuilder.and(query, goTerm("alpha-1,6-mannosyltransferase activity"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_4));
    }

    @Test
    void goPartial1Comma6() {
        String query = goTerm("mannosyltransferase");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_4));
    }

    @Test
    void goExactWithArrowsWhoEverMadeThisStuffUp() {
        String query = goTerm("(1->3)-beta-D-glucan receptor activity");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_5));
    }

    @Test
    void goPartialHyphens() {
        String query = goTerm("beta-D-glucan");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_5));
    }

    @Test
    void goPartialHyphensAlternative() {
        String query = goTerm("beta-D");
        query = QueryBuilder.and(query, goTerm("glucan"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_5));
    }

    @Test
    void goFindMultipleReceptors() {
        String query = goTerm("receptor");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(GO_5, GO_6));
    }

    @Test
    void goFindMultipleReceptorActivities() {
        String query = goTerm("receptor activity");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(GO_5, GO_6));
    }

    private String goTerm(String term) {
        return query(searchEngine.getSearchFieldConfig().getSearchFieldItemByName("go"), term);
    }

    private static final String GO_DYNAMIC_PREFIX = "go_";

    static String goTerm(String goEvidenceType, String value) {
        String field = GO_DYNAMIC_PREFIX + goEvidenceType.toLowerCase();
        return QueryBuilder.query(field, value);
    }
}
