package uk.ac.ebi.uniprot.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.domain.uniparc.UniParcDatabaseType;
import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.search.field.UniParcField;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.DbReferenceType;
import uk.ac.ebi.uniprot.xml.jaxb.uniparc.Entry;

/**
 * Tests the search capabilities of the {@link uk.ac.ebi.uniprot.dataservice.client.uniparc.UniParcQueryBuilder} when
 * it comes to searching for UniParc entries using a taxonomic Identifier or an organism name
 */
public class OrganismSearchIT {
    @ClassRule
    public static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String HUMAN_SCIENTIFIC_NAME = "Homo sapiens";
    private static final String HUMAN_COMMON_NAME = "human";
    private static final int HUMAN_TAX_ID = 9606;
    private static final String EGGPLANT_SCIENTIFIC_NAME = "Solanum melongena";
    private static final int EGGPLANT_TAX_ID = 4111;

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        //Entry 1
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("NCBI_taxonomy_id", ""+HUMAN_TAX_ID));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
            
        
        }

        //Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("NCBI_taxonomy_id", ""+EGGPLANT_TAX_ID));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        searchEngine.printIndexContents();
    }

    @Test
    public void searchNonExistentTaxIdReturns0Documents() throws Exception {
        String query = taxonId(Integer.MAX_VALUE);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    public void searchForHumanTaxIdReturnsEntry1() throws Exception {
        String query = taxonId(HUMAN_TAX_ID);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void searchForHumanScientificNameReturnsEntry1() throws Exception {
        String query = organismName(HUMAN_SCIENTIFIC_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void searchForHumanCommonNameReturnsEntry1() throws Exception {
        String query = organismName(HUMAN_COMMON_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void searchForPartialHumanCommonNameReturnsEntry1() throws Exception {
        String query = organismName("homo");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    public void searchForChondromycesTaxIdReturnsEntry2() throws Exception {
        String query = taxonId(EGGPLANT_TAX_ID);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    @Test
    public void searchForChondromycesScientificNameReturnsEntry2() throws Exception {
        String query = organismName(EGGPLANT_SCIENTIFIC_NAME);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    @Test
    public void searchForNonExistentScientificNameReturns0Entires() throws Exception {
        String query = organismName("Unknown");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }
    private String taxonId(int value) {
    	return QueryBuilder.query(UniParcField.Search.taxonomy_id.name(), ""+value);
    }
    private String organismName(String value) {
    	return QueryBuilder.query(UniParcField.Search.taxonomy_name.name(),value);
    }
}