package org.uniprot.store.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;

class IdentifierSearchIT {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String ID_3 = "UPI0000000003";
    private static final String ID_4 = "UPI0000000004";

    @BeforeAll
    static void populateIndexWithTestData() {
        // a test entry object that can be modified and added to index
        Entry entry = TestUtils.createDefaultUniParcEntry();

        // Entry 1
        entry.setAccession(ID_1);
        searchEngine.indexEntry(entry);

        // Entry 2
        entry.setAccession(ID_2);
        searchEngine.indexEntry(entry);

        // Entry 3
        entry.setAccession(ID_3);
        searchEngine.indexEntry(entry);

        searchEngine.printIndexContents();
    }

    @Test
    void searchNonExistentIdReturns0Documents() {
        String query = id(ID_4);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForIDFromEntry1MatchesEntry1() {
        String query = id(ID_1);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void uppercaseSearchForIDFromEntry1MatchesEntry1() {
        String query = id(ID_1.toUpperCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void lowercaseSearchForIDFromEntry1MatchesEntry1() {
        String query = id(ID_1.toLowerCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForIDFromEntry3MatchesEntry3() {
        String query = id(ID_3);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_3));
    }

    private String id(String value) {
        return QueryBuilder.query(UniParcField.Search.upi.name(), value.toUpperCase());
    }
}
