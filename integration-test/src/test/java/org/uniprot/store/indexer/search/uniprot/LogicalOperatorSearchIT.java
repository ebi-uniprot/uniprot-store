package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests whether the logical operators of the UniProtQueryBuilder are working properly
 */
public class LogicalOperatorSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
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
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    public void queryForAccessionInEntry1ReturnsEntry1() throws Exception {
        String query = query(UniProtField.Search.accession, ACCESSION1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    public void notEntry1ReturnsEntry2And3() throws Exception {
        String query = QueryBuilder.query(UniProtField.Search.accession.name(), ACCESSION1, false, true);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2, ACCESSION3));
    }

    @Test
    public void orAccessionsReturnsEntry1And3() throws Exception {
        String acc1Query = query(UniProtField.Search.accession, ACCESSION1);
        String acc3Query = query(UniProtField.Search.accession, ACCESSION3);

        String orQuery = QueryBuilder.or(acc1Query, acc3Query);

        QueryResponse response = searchEngine.getQueryResponse(orQuery);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3));
    }

    @Test
    public void andAccessionsReturnsEntryNone() throws Exception {
        String acc1Query = query(UniProtField.Search.accession, ACCESSION1);
        String acc3Query = query(UniProtField.Search.accession, ACCESSION3);

        String andQuery = QueryBuilder.and(acc1Query, acc3Query);

        QueryResponse response = searchEngine.getQueryResponse(andQuery);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }
}
