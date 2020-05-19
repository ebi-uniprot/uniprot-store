package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
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
import org.uniprot.store.search.field.QueryBuilder;

/** Tests whether the searches for organism host are working correctly */
class OrganismHostIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String ACCESSION1 = "Q197F4";
    private static final String SCIENTIFIC_NAME1 = "Cercopithecus hamlyni";
    private static final String COMMON_NAME1 = "Owl-faced monkey";
    private static final String SYNONYM1 = "Hamlyn's monkey";
    private static final String MNEMONIC1 = "CERHA";
    private static final int TAX_ID1 = 9536;

    private static final String ACCESSION2 = "Q197F5";
    private static final String SCIENTIFIC_NAME2 = "Callithrix";
    private static final int TAX_ID2 = 9481;

    private static final String ACCESSION3 = "Q197F6";
    private static final String SCIENTIFIC_NAME3 = "Lagothrix lagotricha";
    private static final String COMMON_NAME3 = "Brown woolly monkey";
    private static final String SYNONYM3 = "Humboldt's woolly monkey";
    private static final String MNEMONIC3 = "LAGLA";
    private static final int TAX_ID3 = 9519;

    private static final String ACCESSION4 = "Q197F7";
    private static final String SCIENTIFIC_NAME4_1 = "Canis lupus familiaris";
    private static final String COMMON_NAME4_1 = "Dog";
    private static final String SYNONYM4_1 = "Canis familiaris";
    private static final String MNEMONIC4_1 = "CANLF";
    private static final int TAX_ID4_1 = 9615;
    private static final String SCIENTIFIC_NAME4_2 = "Homo sapiens";
    private static final String COMMON_NAME4_2 = "Human";
    private static final String MNEMONIC4_2 = "HUMAN";
    private static final int TAX_ID4_2 = 9606;
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID1, SCIENTIFIC_NAME1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID2, SCIENTIFIC_NAME2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID3, SCIENTIFIC_NAME3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 4
        String OHLine1 = createOHLine(TAX_ID4_1, SCIENTIFIC_NAME4_1);
        String OHLine2 = createOHLine(TAX_ID4_2, SCIENTIFIC_NAME4_2);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.OH, concatMultiOHLines(OHLine1, OHLine2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    /**
     * Converts the given parameters into the flat file representation of an OH line
     *
     * @param taxId the NCBI taxonomic identifier of the organism host
     * @param officialName the official name of the organism host
     * @return FF representation of the organism host
     */
    private static String createOHLine(int taxId, String officialName) {
        return "OH   NCBI_TaxID=" + taxId + "; " + officialName + ".\n";
    }

    private static String concatMultiOHLines(String... OHLines) {
        StringBuilder finalOHLine = new StringBuilder();

        for (String OHLine : OHLines) {
            finalOHLine.append(OHLine);
        }

        return finalOHLine.toString();
    }

    @Test
    void noMatchesForNonExistentOrganismHostName() {
        String query = organismHostName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void organismHostNameFromEntry1MatchesEntry1() {
        String query = organismHostName(SCIENTIFIC_NAME1);
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME1));
        query = QueryBuilder.and(query, organismHostName(SYNONYM1));
        query = QueryBuilder.and(query, organismHostName(MNEMONIC1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void organismHostNameFromEntry2MatchesEntry2() {
        String query = organismHostName(SCIENTIFIC_NAME2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void organismHostNameFromEntry3MatchesEntry3() {
        String query = organismHostName(SCIENTIFIC_NAME3);
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME3));
        query = QueryBuilder.and(query, organismHostName(SYNONYM3));
        query = QueryBuilder.and(query, organismHostName(MNEMONIC3));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void organismHostNamesFromEntry4MatchesEntry4() {
        String query = organismHostName(SCIENTIFIC_NAME4_1);
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME4_1));
        query = QueryBuilder.and(query, organismHostName(SYNONYM4_1));
        query = QueryBuilder.and(query, organismHostName(MNEMONIC4_1));
        query = QueryBuilder.and(query, organismHostName(SCIENTIFIC_NAME4_2));
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME4_2));
        query = QueryBuilder.and(query, organismHostName(MNEMONIC4_2));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    void organismHostNameFromMultiLineEntryMatchesEntry4() {
        String query = organismHostName(SCIENTIFIC_NAME4_2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    void partialOrganismHostNameMatchesEntry3() {
        String query = organismHostName("lagotricha");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void organismHostMnemonicFromEntry3MatchesEntry3() {
        String query = organismHostName(MNEMONIC3);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void commonPartialOrganismHostNameMatchesEntry1And3() {
        String query = organismHostName("monkey");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    void noMatchesForNonExistentOrganismHostID() {
        String query = organismHostID(9999999);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void organismHostTaxIdFromEntry2MatchesEntry2() {
        String query = organismHostID(TAX_ID2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void firstOrganismHostTaxIdFromMultipleOHEntryMatchesEntry4() {
        String query = organismHostID(TAX_ID4_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    void secondOrganismHostTaxIdFromMultipleOHEntryMatchesEntry4() {
        String query = organismHostID(TAX_ID4_2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    String organismHostName(String name) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("virus_host_name"),
                name);
    }

    String organismHostID(int tax) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("virus_host_id"),
                String.valueOf(tax));
    }
}
