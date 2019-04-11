package uk.ac.ebi.uniprot.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.search.field.UniProtField;

/**
 * Tests whether the searches for organism host are working correctly
 */
public class OrganismHostIT {
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
    @ClassRule
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID1, SCIENTIFIC_NAME1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID2, SCIENTIFIC_NAME2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.OH, createOHLine(TAX_ID3, SCIENTIFIC_NAME3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 4
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
     * @param taxId          the NCBI taxonomic identifier of the organism host
     * @param officialName   the official name of the organism host
     * @return FF representation of the organism host
     */
    private static String createOHLine(int taxId, String officialName) {
        return "OH   NCBI_TaxID=" + taxId + "; " + officialName + ".\n";
    }

    private static String concatMultiOHLines(String... OHLines) {
        String finalOHLine = "";

        for (String OHLine : OHLines) {
            finalOHLine += OHLine;
        }

        return finalOHLine;
    }

    @Test
    public void noMatchesForNonExistentOrganismHostName() throws Exception {
        String query = organismHostName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void organismHostNameFromEntry1MatchesEntry1() throws Exception {
        String query = organismHostName(SCIENTIFIC_NAME1);
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME1));
        query = QueryBuilder.and(query,organismHostName(SYNONYM1));
        query = QueryBuilder.and(query,organismHostName(MNEMONIC1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void organismHostNameFromEntry2MatchesEntry2() throws Exception {
        String query = organismHostName(SCIENTIFIC_NAME2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void organismHostNameFromEntry3MatchesEntry3() throws Exception {
        String query = organismHostName(SCIENTIFIC_NAME3);
        query = QueryBuilder.and(query, organismHostName(COMMON_NAME3));
        query = QueryBuilder.and(query, organismHostName(SYNONYM3));
        query = QueryBuilder.and(query, organismHostName(MNEMONIC3));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void organismHostNamesFromEntry4MatchesEntry4() throws Exception {
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
    public void organismHostNameFromMultiLineEntryMatchesEntry4() throws Exception {
        String query = organismHostName(SCIENTIFIC_NAME4_2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void partialOrganismHostNameMatchesEntry3() throws Exception {
        String query = organismHostName("lagotricha");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void organismHostMnemonicFromEntry3MatchesEntry3() throws Exception {
        String query = organismHostName(MNEMONIC3);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void commonPartialOrganismHostNameMatchesEntry1And3() throws Exception {
        String query = organismHostName("monkey");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    public void noMatchesForNonExistentOrganismHostID() throws Exception {
        String query = organismHostID(9999999);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void organismHostTaxIdFromEntry2MatchesEntry2() throws Exception {
        String query = organismHostID(TAX_ID2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void firstOrganismHostTaxIdFromMultipleOHEntryMatchesEntry4() throws Exception {
        String query = organismHostID(TAX_ID4_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void secondOrganismHostTaxIdFromMultipleOHEntryMatchesEntry4() throws Exception {
        String query = organismHostID(TAX_ID4_2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }
    
    String organismHostName(String name) {
    	return query(UniProtField.Search.host_name, name);
    }
    String organismHostID(int tax) {
    	return query(UniProtField.Search.host_id, String.valueOf(tax));
    }
}