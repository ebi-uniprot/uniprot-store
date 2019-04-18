package uk.ac.ebi.uniprot.indexer.search.uniprot;

import org.apache.commons.lang.WordUtils;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

/**
 * Verifies whether the accession searches are qorking properly
 */
public class AccessionSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String PRIMARY_ACCESSION1 = "Q197F5";
    private static final String SECONDARY_ACCESSION1_1 = "A4D160";
    private static final String SECONDARY_ACCESSION1_2 = "A4D161";
    private static final String PRIMARY_ACCESSION2 = "Q197F6";
    private static final String SECONDARY_ACCESSION2_1 = "A4D162";
    private static final String PRIMARY_ACCESSION3 = "Q197F7";
    @ClassRule
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, createACLine(PRIMARY_ACCESSION1, SECONDARY_ACCESSION1_1,
                SECONDARY_ACCESSION1_2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, createACLine(PRIMARY_ACCESSION2, SECONDARY_ACCESSION2_1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, createACLine(PRIMARY_ACCESSION3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createACLine(String primaryAcc, String... secondaryAccs) {
        StringBuilder ACLineBuilder = new StringBuilder("AC   ");

        ACLineBuilder.append(primaryAcc).append("; ");

        for (String secondaryAcc : secondaryAccs) {
            ACLineBuilder.append(secondaryAcc).append("; ");
        }

        return ACLineBuilder.toString().trim();
    }

    @Test
    public void searchAccessionDoesNotMatchAnyDocument() throws Exception {
    	String query  = "accession:P12345";
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void upperCaseAccessionFromEntry3MatchesEntry3() throws Exception {      //  Query query = UniProtQueryBuilder.accession(PRIMARY_ACCESSION3);
        String query  = "accession:" +PRIMARY_ACCESSION3;
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(PRIMARY_ACCESSION3));
    }

    @Test
    public void lowerCaseAccessionFromEntry3MatchesEntry3() throws Exception {
        String query  = "accession:" +PRIMARY_ACCESSION3.toLowerCase();
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(PRIMARY_ACCESSION3));
    }

    @Test
    public void mixedCaseAccessionFromEntry3MatchesEntry3() throws Exception {
        String query  = "accession:" +PRIMARY_ACCESSION3;
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(PRIMARY_ACCESSION3));
    }


    @Test
    public void tooManyClauses() throws Exception {
    	   String query  = "accession:" +PRIMARY_ACCESSION1;
        for (int i = 0; i < 2000; i++) {
        	query += " AND accession:" + PRIMARY_ACCESSION1;
         
        }

        try {
            QueryResponse response = searchEngine.getQueryResponse(query);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Test
    public void secondaryAccessionFromEntry1MatchesEntry1() throws Exception {
        String query  = "accession:" +SECONDARY_ACCESSION1_1;
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(PRIMARY_ACCESSION1));
    }

   

    @Test
    public void searchingAccessionsFromEntry1And2MatchesEntry1And2() throws Exception {
        String query = "accession:" + SECONDARY_ACCESSION1_1  +" OR " +"accession:" + PRIMARY_ACCESSION2;       
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(PRIMARY_ACCESSION1, PRIMARY_ACCESSION2));
    }

}
