package org.uniprot.store.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.domain2.UniProtSearchFields;
import org.uniprot.store.search.field.QueryBuilder;

class SequenceChecksumSearchIT {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String CHECKSUM_1 = "5A0A2229D6C87ABF";
    private static final String CHECKSUM_2 = "76F4826B7009DFAF";

    @BeforeAll
    static void populateIndexWithTestData() {
        // a test entry object that can be modified and added to index
        Entry stubEntryObject = TestUtils.createDefaultUniParcEntry();

        // Entry 1
        stubEntryObject.setAccession(ID_1);
        stubEntryObject.setSequence(TestUtils.createSequence("A", CHECKSUM_1));
        searchEngine.indexEntry(stubEntryObject);

        // Entry 2
        stubEntryObject.setAccession(ID_2);
        stubEntryObject.setSequence(TestUtils.createSequence("B", CHECKSUM_2));
        searchEngine.indexEntry(stubEntryObject);

        searchEngine.printIndexContents();
    }

    @Test
    void searchNonExistentChecksumMatches0Entries() {
        String query = checksum("Unknown");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForChecksumOfEntry1MatchesEntry1() {
        String query = checksum(CHECKSUM_1);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void lowerCaseSearchForChecksumOfEntry1MatchesEntry1() {
        String query = checksum(CHECKSUM_1.toLowerCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void upperCaseSearchForChecksumOfEntry1MatchesEntry1() {
        String query = checksum(CHECKSUM_1.toUpperCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void partialSearchForChecksumOfEntry1Matches0Entries() {
        String query = checksum("5A0A2229D");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForChecksumOfEntry2MatchesEntry2() {
        String query = checksum(CHECKSUM_2);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    private String checksum(String value) {
        return QueryBuilder.query(
                UniProtSearchFields.UNIPARC.getField("checksum").getName(), value);
    }
}
