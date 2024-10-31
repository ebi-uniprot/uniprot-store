package org.uniprot.store.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;

class DatabaseSearchIT {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String ID_3 = "UPI0000000003";
    private static final String ACC_P47986 = "P47986";
    private static final String ACC_P47986_1 = "P47986-1";
    private static final String ACC_P47988 = "P47988";
    private static final String ACC_ENSP00000226587 = "ENSP00000226587";
    private static final Integer ACC_ENSP_VERSION = 5;
    private static final String ACC_NC_000004_1185_0 = "NC_000004_1185_0";
    private static final String ACC_SEED = "fig|1218145.peg.2041";

    @BeforeAll
    static void populateIndexWithTestData() {
        // Entry 1
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            entry.getDbReference().clear();
            entry.getDbReference()
                    .add(TestUtils.createXref(UniParcDatabase.TREMBL.getName(), ACC_P47986, "Y"));
            entry.getDbReference()
                    .add(TestUtils.createXref(UniParcDatabase.SEED.getName(), ACC_SEED, "Y"));
            searchEngine.indexEntry(entry);
        }

        // Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            entry.getDbReference().clear();
            entry.getDbReference()
                    .add(
                            TestUtils.createXref(
                                    UniParcDatabase.ENSEMBL_VERTEBRATE.getName(),
                                    ACC_ENSP00000226587,
                                    "Y",
                                    ACC_ENSP_VERSION));
            entry.getDbReference()
                    .add(
                            TestUtils.createXref(
                                    UniParcDatabase.SWISSPROT_VARSPLIC.getName(),
                                    ACC_P47986_1,
                                    "Y"));
            searchEngine.indexEntry(entry);
        }

        // Entry 3
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_3);
            entry.getDbReference().clear();
            entry.getDbReference()
                    .add(TestUtils.createXref(UniParcDatabase.TREMBL.getName(), ACC_P47988, "N"));
            entry.getDbReference()
                    .add(
                            TestUtils.createXref(
                                    UniParcDatabase.REFSEQ.getName(), ACC_NC_000004_1185_0, "Y"));
            searchEngine.indexEntry(entry);
        }

        searchEngine.printIndexContents();
    }

    @Test
    void testAllUniProt() {
        String query = uniprot("*");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(1, retrievedAccessions.size()); // no inactive entry
        assertThat(retrievedAccessions, containsInAnyOrder(ID_1));
    }

    @Test
    void testAllUniProtIsoform() {
        String query = isoform("*");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(2, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2));
    }

    @Test
    void testActiveTrembl() {
        String query = active("Uniprot");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
        System.out.println(retrievedAccessions);
        assertEquals(1, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_1));
    }

    @Test
    void testDBUniProt() {
        String query = database("Uniprot");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(2, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_3));
    }

    @Test
    void testDBREFSEQ() {
        String query = database(UniParcDatabase.REFSEQ.getName());

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(1, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_3));
    }

    @Test
    void testDbIdWithoutVersionFound() {
        String query = dbId(ACC_ENSP00000226587);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(1, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_2));
    }

    @Test
    void testDbIdWithVersionFound() {
        String query = dbId(ACC_ENSP00000226587 + "." + ACC_ENSP_VERSION);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(1, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_2));
    }

    @Test
    void testDbIdWithInvalidVersionNotFound() {
        String query = dbId(ACC_ENSP00000226587 + ".2");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(0, retrievedAccessions.size());
    }

    @Test
    void testDbIdWithMultipleDotsFound() {
        String query = dbId(ACC_SEED);

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(1, retrievedAccessions.size());
        assertThat(retrievedAccessions, containsInAnyOrder(ID_1));
    }

    @Test
    void testDbIdWithMultipleDotsPartialNotFound() {
        String query = dbId("fig|1218145");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(0, retrievedAccessions.size());
    }

    @Test
    void testDbIdWithMultipleDotsPartialEndNotFound() {
        String query = dbId("peg.2041");

        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

        assertEquals(0, retrievedAccessions.size());
    }

    private String active(String dbname) {
        return QueryBuilder.query(
                searchEngine
                        .getSearchFieldConfig()
                        .getSearchFieldItemByName("active")
                        .getFieldName(),
                dbname);
    }

    private String dbId(String idId) {
        return QueryBuilder.query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("dbid").getFieldName(),
                idId);
    }

    private String database(String dbname) {
        return QueryBuilder.query(
                searchEngine
                        .getSearchFieldConfig()
                        .getSearchFieldItemByName("database")
                        .getFieldName(),
                dbname);
    }

    String uniprot(String acc) {
        return QueryBuilder.query(
                searchEngine
                        .getSearchFieldConfig()
                        .getSearchFieldItemByName("accession")
                        .getFieldName(),
                acc);
    }

    String isoform(String acc) {
        return QueryBuilder.query(
                searchEngine
                        .getSearchFieldConfig()
                        .getSearchFieldItemByName("isoform")
                        .getFieldName(),
                acc);
    }
}
