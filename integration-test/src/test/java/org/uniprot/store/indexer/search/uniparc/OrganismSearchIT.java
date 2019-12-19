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
import org.uniprot.core.uniparc.UniParcDatabaseType;
import org.uniprot.core.xml.jaxb.uniparc.DbReferenceType;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;

class OrganismSearchIT {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String HUMAN_SCIENTIFIC_NAME = "Homo sapiens";
    private static final String HUMAN_COMMON_NAME = "human";
    private static final int HUMAN_TAX_ID = 9606;
    private static final String EGGPLANT_SCIENTIFIC_NAME = "Solanum melongena";
    private static final int EGGPLANT_TAX_ID = 4111;

    @BeforeAll
    static void populateIndexWithTestData() {
        // Entry 1
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("NCBI_taxonomy_id", "" + HUMAN_TAX_ID));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty()
                    .add(TestUtils.createProperty("NCBI_taxonomy_id", "" + EGGPLANT_TAX_ID));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        searchEngine.printIndexContents();
    }

    @Test
    void searchNonExistentTaxIdReturns0Documents() {
        String query = taxonId(Integer.MAX_VALUE);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForHumanTaxIdReturnsEntry1() {
        String query = taxonId(HUMAN_TAX_ID);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForHumanScientificNameReturnsEntry1() {
        String query = organismName(HUMAN_SCIENTIFIC_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForHumanCommonNameReturnsEntry1() {
        String query = organismName(HUMAN_COMMON_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForPartialHumanCommonNameReturnsEntry1() {
        String query = organismName("homo");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForChondromycesTaxIdReturnsEntry2() {
        String query = taxonId(EGGPLANT_TAX_ID);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    @Test
    void searchForChondromycesScientificNameReturnsEntry2() {
        String query = organismName(EGGPLANT_SCIENTIFIC_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    @Test
    void searchForNonExistentScientificNameReturns0Entires() {
        String query = organismName("Unknown");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    private String taxonId(int value) {
        return QueryBuilder.query(UniParcField.Search.taxonomy_id.name(), "" + value);
    }

    private String organismName(String value) {
        return QueryBuilder.query(UniParcField.Search.taxonomy_name.name(), value);
    }
}
