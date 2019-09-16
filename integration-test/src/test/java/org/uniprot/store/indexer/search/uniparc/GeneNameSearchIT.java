package org.uniprot.store.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.uniparc.UniParcDatabaseType;
import org.uniprot.core.xml.jaxb.uniparc.DbReferenceType;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;


class GeneNameSearchIT {
    @RegisterExtension
    static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String ID_3 = "UPI0000000003";
    private static final String ID_4 = "UPI0000000004";
    private static final String GN_ZNF705G = "ZNF705G";
    private static final String GN_HLA_A = "HLA-A";
    private static final String GN_HLA_B = "HLA-B";
    private static final String GN_LONG = "AMTR_s00092p00144240";

    @BeforeAll
    static void populateIndexWithTestData() {
        //Entry 1

        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            entry.getDbReference().clear();
            
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("gene_name", GN_ZNF705G));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        
        //Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            entry.getDbReference().clear();
            
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("gene_name", GN_HLA_A));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }
       

        //Entry 3
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_3);
            entry.getDbReference().clear();
            
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("gene_name", GN_HLA_B));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }


        //Entry 4
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_4);
            entry.getDbReference().clear();
            
            DbReferenceType xref= TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("gene_name", GN_LONG));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

    }

    @Test
    void searchNonExistentIdReturns0Entries() {
        String query = gene("Unknown");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForZNF705GMatchesEntry1() {
        String query = gene(GN_ZNF705G);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void lowerCaseSearchForZNF705GMatchesEntry1() {
        String query = gene(GN_ZNF705G.toLowerCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void upperCaseSearchForZNF705GMatchesEntry1() {
        String query = gene(GN_ZNF705G.toUpperCase());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_1));
    }

    @Test
    void searchForHLA_AMatchesEntry2() {
        String query = gene(GN_HLA_A);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_2));
    }

    @Test
    void searchForHLA_BMatchesEntry3() {
        String query = gene(GN_HLA_B);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_3));
    }

    @Test
    void partialSearchForHLAMatchesEntry2And3() {
        String query = gene("HLA");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, containsInAnyOrder(ID_2, ID_3));
    }

    @Test
    void partialSearchWithLessThan3CharsMatches0Entries() {
        String query = gene("HL");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, is(empty()));
    }

    @Test
    void searchForLongGeneNameMatchesEntry4() {
        String query = gene(GN_LONG);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_4));
    }

    @Test
    void partialSearchForLongGeneNameStartingFromMiddleOfNameMatches0Entries() {
        String query = gene("s00092p00144240");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedIdentifiers = searchEngine.getIdentifiers(response);
        assertThat(retrievedIdentifiers, contains(ID_4));
    }
    private String gene(String value) {
    	return QueryBuilder.query(UniParcField.Search.gene.name(),value);
    }
    
}