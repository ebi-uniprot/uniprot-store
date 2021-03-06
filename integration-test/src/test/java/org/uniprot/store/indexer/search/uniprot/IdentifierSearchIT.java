package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.commons.lang.WordUtils;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;

/** Verifies if the protein accession/protein id is indexed correctly */
class IdentifierSearchIT {
    static final String ACC_LINE = "AC   %s;";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String TARGET_ACCESSION = "Q197F5";
    private static final String TARGET_ID = "CYC_HUMAN";
    private static final String ID_LINE = "ID   %s               Reviewed;         105 AA.";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, "Q197F4"));
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, "CYC_PANTR"));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, TARGET_ACCESSION));
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, TARGET_ID));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, "Q197F6"));
        entryProxy.updateEntryObject(LineType.ID, String.format(ID_LINE, "AATM_RABIT"));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void upperCaseSearchProteinIdMatchesTargetDocument() {
        String query = id(TARGET_ID);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, IsIterableContainingInOrder.contains(TARGET_ACCESSION));
    }

    @Test
    void lowerCaseSearchProteinIdMatchesTargetDocument() {
        String query = id(TARGET_ID.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, IsIterableContainingInOrder.contains(TARGET_ACCESSION));
    }

    @Test
    void mixedCaseSearchProteinIdMatchesTargetDocument() {
        String query = id(mixCasing(TARGET_ID));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, IsIterableContainingInOrder.contains(TARGET_ACCESSION));
    }

    @Test
    void searchProteinIdDoesNotMatchAnyDocument() {
        String query = id("IES3_YEAST");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void partialProteinIdMatches0Documents() {
        String query = id("CYC");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchForAccessionInIdQueryReturns0Documents() {
        String query = id(TARGET_ACCESSION);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    private String id(String id) {
        return QueryBuilder.query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("id").getFieldName(),
                id);
    }

    private String mixCasing(String value) {
        return WordUtils.capitalize(value);
    }
}
