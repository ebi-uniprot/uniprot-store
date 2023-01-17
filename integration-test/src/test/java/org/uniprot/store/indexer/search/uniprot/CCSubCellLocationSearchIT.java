package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;

class CCSubCellLocationSearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZY3 = "Q6GZY3";
    private static final String Q197B6 = "Q197B6";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";

    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SUBCELLULAR LOCATION: This-is-a-word Host membrane extraWord {ECO:0000305}; Single-pass\n"
                        + "CC       membrane protein {ECO:0000305}.\n"
                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Absorption:\n"
                        + "CC         Abs(max)=~715 nm;\n"
                        + "CC         Note=Emission maxima at 735 nm. {ECO:0000269|PubMed:11553743};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SUBCELLULAR LOCATION: This-is-a Host membrane; Single-pass\n"
                        + "CC       membrane protein. Note=Localizes endoplasmic at mid-cell.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SUBCELLULAR LOCATION: [Spike protein S2]: Virion membrane\n"
                        + "CC       {ECO:0000313|EMBL:BAG16761.1}; Single-pass type I membrane sdssds\n"
                        + "CC       protein (By similarity) {ECO:0000269|PubMed:10433554}. Host\n"
                        + "CC       endoplasmic reticulum-Golgi intermediate compartment membrane\n"
                        + "CC       {ECO:0000303|Ref.6}; Type I me (By similarity)\n"
                        + "CC       {ECO:0000313|PDB:3OW2}; Another top {ECO:0000313|EMBL:BAG16761.1}.\n"
                        + "CC       Note=Accumulates in the endoplasmic reticulum-Golgi intermediate\n"
                        + "CC       compartment, where it participates in virus particle assembly.\n"
                        + "CC       Some S oligomers may be transported to the plasma membrane, where\n"
                        + "CC       they may mediate cell fusion (By similarity).\n"
                        + "CC       {ECO:0000256|HAMAP-Rule:MF_00205}.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void termThree() {
        String value = "membrane";
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("cc_scl_term"),
                        value);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q6GZX3, Q6GZY3));
    }

    @Test
    void termTopologyThree() {
        String value = "protein";
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("cc_scl_term"),
                        value);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q6GZX3, Q6GZY3));
    }

    @Test
    void termOrientationOne() {
        String value = "top";
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("cc_scl_term"),
                        value);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6));
    }

    @Test
    void noteTwo() {
        String value = "endoplasmic";
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("cc_scl_note"),
                        value);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q6GZY3));
    }
}
