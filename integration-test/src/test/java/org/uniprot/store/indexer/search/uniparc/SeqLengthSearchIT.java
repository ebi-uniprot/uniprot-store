package org.uniprot.store.indexer.search.uniparc;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.collection.IsEmptyCollection.empty;

class SeqLengthSearchIT {
    @RegisterExtension
    static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String CHECKSUM_1 = "5A0A2229D6C87ABF";
    private static final String CHECKSUM_2 = "76F4826B7009DFAF";

    @BeforeAll
    static void populateIndexWithTestData() {
        // a test entry object that can be modified and added to index
        Entry entry = TestUtils.createDefaultUniParcEntry();

        //Entry 1
        entry.setAccession(ID_1);
        entry.setSequence(TestUtils.createSequence("ABASFAFFFSAFAAFA", CHECKSUM_1));
        searchEngine.indexEntry(entry);

        //Entry 2
        entry.setAccession(ID_2);
        entry.setSequence(TestUtils.createSequence("DFDFASDFAFSADFASDFSFSAFSAFSDF", CHECKSUM_2));
        searchEngine.indexEntry(entry);

        searchEngine.printIndexContents();
    }
    @Test
    void searchOnExactLengthWithEntryOne(){
        String query = seqLength(16);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }
    @Test
    void searchOnExactLengthWithoutAny(){
        String query = seqLength(15);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));

         query = seqLength(30);
         response = searchEngine.getQueryResponse(query);

         retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchOnLengthRangWithOneEntry(){
        String query = seqLengthRange(15, 18);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }
    @Test
    void searchOnLengthRangWithTwoEntries(){
        String query = seqLengthRange(15, 30);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, containsInAnyOrder(ID_1, ID_2));
    }

    @Test
    void searchOnLengthRangWithoutAny(){
        String query = seqLengthRange(5, 15);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));

        query = seqLengthRange(30, 40);
        response = searchEngine.getQueryResponse(query);

        retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }
    private String seqLengthRange(int start, int end) {
    	return QueryBuilder.rangeQuery(UniParcField.Search.length.name(), start, end);
    }
    
    private String seqLength(int start) {
    	return QueryBuilder.query(UniParcField.Search.length.name(), ""+start);
    }
}
