package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

/**
 * Verifies whether the accession searches are qorking properly
 */
class IsoformIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String PRIMARY_ACCESSION1 = "Q197F5-1";
    private static final String SECONDARY_ACCESSION1_1 = "A4D160";
    private static final String SECONDARY_ACCESSION1_2 = "A4D161";
    private static final String PRIMARY_ACCESSION2 = "Q197F6";
    private static final String SECONDARY_ACCESSION2_1 = "A4D162";
    private static final String PRIMARY_ACCESSION3 = "Q197F7-2";
    @RegisterExtension
    static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
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
    void queryIsofromAccessionFromEntry3MatchesEntry3() {
        String query = accession(PRIMARY_ACCESSION3.toLowerCase());
        String query1 = isoformOnly();
        String and = QueryBuilder.and(query, query1);

        QueryResponse response = searchEngine.getQueryResponse(and);

        List<String> identifiers = searchEngine.getIdentifiers(response);
        assertThat(identifiers, contains(PRIMARY_ACCESSION3));
    }

    @Test
    void queryIsofromAccessionFromEntry2MatchesEntry() {
        String query = accession(PRIMARY_ACCESSION2.toLowerCase());
        String query1 = isoformOnly();
        String and = QueryBuilder.and(query, query1);

        QueryResponse response = searchEngine.getQueryResponse(and);

        List<String> identifiers = searchEngine.getIdentifiers(response);
        assertThat(identifiers, empty());
    }

    private String accession(String accession) {
    	return query(UniProtField.Search.accession, accession);
    }
    private String isoformOnly() {
    	return query(UniProtField.Search.is_isoform, "true");
    }
}
