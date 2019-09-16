package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprot.description.FlagType;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests if the fragment search is working correctly
 */
public class FragmentSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String ACCESSION1 = "Q197F4";
    private static final String ACCESSION2 = "Q197F5";
    private static final String ACCESSION3 = "Q197F6";
    @RegisterExtension
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.DE, createDELineWithFragment(FlagType.FRAGMENT.getValue()));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.DE, createDELineWithFragment(FlagType.FRAGMENTS.getValue()));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.DE, createDELineWithFragment(FlagType.PRECURSOR.getValue()));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createDELineWithFragment(String fragment) {
        String deLine = "DE   RecName: Full=DUMMY;\n";

        if (fragment != null) {
            deLine += "DE   Flags: " + fragment + ";\n";
        }

        return deLine;
    }

    @Test
    public void searchForNonFragmentProteinsHitsEntry3() throws Exception {
        String query = query(UniProtField.Search.fragment , "false");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }
    
    @Test
    public void searchForPrecursorProteinsHitsEntry3() throws Exception {
        String query =  query(UniProtField.Search.precursor, "true");
   
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    public void searchForFragmentProteinsHitsEntry1And2() throws Exception {
        String query = query(UniProtField.Search.fragment , "true");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2));
    }
}
