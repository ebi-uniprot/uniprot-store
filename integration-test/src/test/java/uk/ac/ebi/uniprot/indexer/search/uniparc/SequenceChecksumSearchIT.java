package uk.ac.ebi.uniprot.indexer.search.uniparc;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.search.field.UniParcField;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

/**
 * Tests the search capabilities of the {@link UniParcQueryBuilder} when it comes to searching for UniParc entries
 * that have a given checksum
 */
public class SequenceChecksumSearchIT {
    @ClassRule
    public static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String CHECKSUM_1 = "5A0A2229D6C87ABF";
    private static final String CHECKSUM_2 = "76F4826B7009DFAF";

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        Entry stubEntryObject = TestUtils.createDefaultUniParcEntry();

        //Entry 1
        stubEntryObject.setAccession(ID_1);
        stubEntryObject.setSequence(TestUtils.createSequence("A", CHECKSUM_1));
        searchEngine.indexEntry(stubEntryObject);

        //Entry 2
        stubEntryObject.setAccession(ID_2);
        stubEntryObject.setSequence(TestUtils.createSequence("B", CHECKSUM_2));
        searchEngine.indexEntry(stubEntryObject);

        searchEngine.printIndexContents();
    }

    @Test
    public void searchNonExistentChecksumMatches0Entries() throws Exception {
        String query=checksum("Unknown");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    public void searchForChecksumOfEntry1MatchesEntry1() throws Exception {
        String query=checksum(CHECKSUM_1);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void lowerCaseSearchForChecksumOfEntry1MatchesEntry1() throws Exception {
        String query=checksum(CHECKSUM_1.toLowerCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void upperCaseSearchForChecksumOfEntry1MatchesEntry1() throws Exception {
        String query=checksum(CHECKSUM_1.toUpperCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void partialSearchForChecksumOfEntry1Matches0Entries() throws Exception {
        String query=checksum("5A0A2229D");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    public void searchForChecksumOfEntry2MatchesEntry2() throws Exception {
        String query=checksum(CHECKSUM_2);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }
    private String checksum(String value) {
    	return QueryBuilder.query(UniParcField.Search.checksum.name(),value);
    }
}
