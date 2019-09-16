package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests if the EC numbers are searched correctly
 */
public class ECSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String ACCESSION0 = "Q197F4";
    private static final String DE_LINE0 =
            "DE   RecName: Full=DUMMY;\n" +
                    "DE            EC=1.1.1.1;\n" +
                    "DE   AltName: Full=DUMMY;\n" +
                    "DE            EC=2.2.2.2;\n" +
                    "DE   SubName: Full=DUMMY;\n" +
                    "DE            EC=3.3.3.3;\n" +
                    "DE   Includes:\n" +
                    "DE     RecName: Full=DUMMY;\n" +
                    "DE              EC=4.4.4.4;\n" +
                    "DE     AltName: Full=DUMMY;\n" +
                    "DE              EC=5.5.5.5;\n" +
                    "DE   Contains:\n" +
                    "DE     RecName: Full=DUMMY;\n" +
                    "DE              EC=6.6.6.6;\n" +
                    "DE     AltName: Full=DUMMY;\n" +
                    "DE              EC=7.7.7.7;\n";

    private static final String ACCESSION1 = "Q197F5";
    private static final String EC1 = "3.4.11.4";

    private static final String ACCESSION2 = "Q197F6";
    private static final String EC2 = "3.4.11.5";

    private static final String ACCESSION3 = "Q197F7";
    private static final String EC3 = "3.4.11.-";

    private static final String ACCESSION4 = "Q197F8";
    private static final String EC4 = "3.4.10.4";

    private static final String ACCESSION5 = "Q197F9";
    private static final String EC5 = "2.4.11.4";

    private static final String ACCESSION6 = "Q198F1";
    private static final String EC6 = "2.4.12.100";

    private static final String ACCESSION7 = "Q198F2";
    private static final String EC7 = "2.4.13.n6";

    private static final String ACCESSION8 = "Q198F3";
    private static final String EC8 = "2.4.14.n26";

    private static final String ACCESSION9 = "Q197G1";
    private static final String EC9 = "2.4.11.40";

    @RegisterExtension
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 0
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION0));
        entryProxy.updateEntryObject(LineType.DE, DE_LINE0);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC4));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 5
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION5));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC5));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 6
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION6));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC6));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 7
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION7));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC7));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 8
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION8));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC8));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 9
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION9));
        entryProxy.updateEntryObject(LineType.DE, createDELine(EC9));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createDELine(String ecNumber) {
        return "DE   RecName: Full=DUMMY;\n" +
                "DE            EC=" + ecNumber + ";\n";
    }

    @Test
    public void noMatchForUnknownEC() throws Exception {
        String query = ec("1.1.1.2");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForECNumberInMainRecMatchesEntry0() throws Exception {
        String query = ec("1.1.1.1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInMainAltMatchesEntry0() throws Exception {
        String query = ec("2.2.2.2");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInMainSubMatchesEntry0() throws Exception {
        String query = ec("3.3.3.3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInIncludesRecMatchesEntry0() throws Exception {
        String query = ec("4.4.4.4");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInIncludesAltMatchesEntry0() throws Exception {
        String query = ec("5.5.5.5");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInContainsRecMatchesEntry0() throws Exception {
        String query = ec("6.6.6.6");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumberInContainsAltMatchesEntry0() throws Exception {
        String query = ec("7.7.7.7");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION0));
    }

    @Test
    public void searchForECNumber3_4_11_4MatchesEntry1() throws Exception {
        String query = ec(EC1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void searchForECNumber3_4_11_5MatchesEntry1() throws Exception {
        String query = ec(EC2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    public void searchForECNumber3_4_11_MatchesEntry1And2And3() throws Exception {
        String query = ec(EC3);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2, ACCESSION3));
    }
    
    @Test
    public void searchForECExactNumber3_4_11_MatchesEntry3() throws Exception {
        String query = query(UniProtField.Search.ec_exact, EC3);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, containsInAnyOrder( ACCESSION3));
    }
    

    @Test
    public void searchForECNumber3_4_MatchesEntry1And2And3And4() throws Exception {
        String query = ec("3.4.-.-");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2, ACCESSION3, ACCESSION4));
    }

    @Test
    public void searchForECNumber3_MatchesEntry0And1And2And3And4() throws Exception {
        String query = ec("3.-.-.-");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION0, ACCESSION1, ACCESSION2, ACCESSION3, ACCESSION4));
    }

    @Test
    public void searchForECNumber3_4_10_4MatchesEntry4() throws Exception {
        String query = ec(EC4);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    public void searchForECNumber2_4_11_4MatchesEntry4() throws Exception {
        String query = ec(EC5);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }

    @Test
    public void searchForECNumber2_4_11_40DoesMatchesEntry9() throws Exception {
        String query = ec(EC9);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION9));
    }

    @Test
    public void searchForECNumber2_4_11_4DoesNotDoWildCardSearchAtTheEndAndMatchEntry9() throws Exception {
        String query = ec(EC5);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, not(contains(ACCESSION9)));
    }

    @Test
    public void searchForECNumber2_4_11_3MatchesEntry4() throws Exception {
        String query = ec("2.4.11.3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForECNumberWithMissingSecondElementDoesNotMatch() throws Exception {
        String query = ec("2.-.11.4");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void searchForECNumber2_4_12_100MatchesEntry6() throws Exception {
        String query = ec(EC6);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    public void searchForECNumber2_4_13_n6MatchesEntry7() throws Exception {
        String query = ec(EC7);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION7));
    }

    @Test
    public void searchForECNumber2_4_14_n26MatchesEntry8() throws Exception {
        String query = ec(EC8);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION8));
    }
    
    @Test
    public void searchForECNumber2_4_11MatchEntry9() throws Exception {
        String query = ec("2.4.11");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION9, ACCESSION5));
    }

    private String ec(String value) {
    	return query(UniProtField.Search.ec, value);
    }
}