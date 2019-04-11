package uk.ac.ebi.uniprot.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.indexer.document.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.*;

/**
 * Verifies if the protein UniProt entry type (Reviewed/Unreviewed) is indexed correctly
 */
public class ReviewedSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String ID_LINE = "ID   CYC_HUMAN               %s;         105 AA.";
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
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, "Reviewed"));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, "Unreviewed"));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, "Reviewed"));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    public void reviewedEntryTypMatchesEntriesWithReviewedStatus() throws Exception {
        String query = query(UniProtField.Search.reviewed, "true");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1, ACCESSION3));
    }

    @Test
    public void unReviewedEntryTypMatchesEntriesWithUnreviewedStatus() throws Exception {
    	 String query = query(UniProtField.Search.reviewed, "false");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

}
