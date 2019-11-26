package org.uniprot.store.indexer.search.uniparc;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.uniparc.UniParcDatabaseType;
import org.uniprot.core.xml.jaxb.uniparc.DbReferenceType;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;

class ProteinNameSearchIT {
    @RegisterExtension static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String ID_3 = "UPI0000000003";
    private static final String ID_4 = "UPI0000000004";
    private static final String ID_5 = "UPI0000000005";
    private static final String ID_6 = "UPI0000000006";
    private static final String NAME_1 = "hypothetical protein";
    private static final String NAME_2 = "Protein kinase C inhibitor KCIP-1 isoform eta";
    private static final String NAME_3 = "14-3-3";
    private static final String NAME_4 = "hydrophobe/amphiphile efflux-1 (HAE1) family transporter";
    private static final String NAME_5 = "PP2A B subunit isoform B'-delta";
    private static final String NAME_6 = "Methylenetetrahydrofolate dehydrogenase (NADP(+))";

    @BeforeAll
    static void populateIndexWithTestData() {
        // a test entry object that can be modified and added to index

        // Entry 1
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_1));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_2));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 3
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_3);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_3));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 4
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_4);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_4));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 5
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_5);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_5));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        // Entry 6
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_6);
            entry.getDbReference().clear();

            DbReferenceType xref =
                    TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), "P47986", "Y");
            xref.getProperty().add(TestUtils.createProperty("protein_name", NAME_6));
            entry.getDbReference().add(xref);
            searchEngine.indexEntry(entry);
        }

        searchEngine.printIndexContents();
    }

    @Test
    void nonExistentProteinNameMatchesNoDocuments() {
        String query = proteinName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchName1HitsEntry1() {
        String query = proteinName(NAME_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_1));
    }

    @Test
    void lowerCaseSearchName1HitsEntry1() {
        String query = proteinName(NAME_1.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_1));
    }

    @Test
    void upperCaseSearchName1HitsEntry1() {
        String query = proteinName(NAME_1.toUpperCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_1));
    }

    @Test
    void partialSearchName1HitsEntry1() {
        String query = proteinName("hypothetical");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_1));
    }

    @Test
    void searchName2HitsEntry2() {
        String query = proteinName("Protein kinase C inhibitor KCIP-1 isoform eta");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_2));
    }

    @Test
    void partialSearchUsingHyphenatedNameInName2HitsEntry2() {
        String query = proteinName("KCIP-1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_2));
    }

    @Test
    void noMatchForPartialSearchUsingHyphenatedNameWithExtraNonExistentCharacter() {
        String query = proteinName("KCIPE-1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void partialSearchUsingHyphenatedNameWithoutTheHyphenHitsEntry2() {
        String query = proteinName("KCIP 1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_2));
    }

    @Test
    void partialSearchForHyphenatedNameWithoutLeftSideOfHyphenationHitsEntry2() {
        String query = proteinName("KCIP");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_2));
    }

    @Test
    void noMatchForPartialSubstringSearchOnHyphenatedName() {
        String query = proteinName("KCI");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchForHyphenatedAndNumberedNameMatchesEntry3() {
        String query = proteinName("14-3-3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_3));
    }

    @Test
    void searchForPartialHyphenatedAndNumberedNameMatchesEntry3() {
        String query = proteinName("14-3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_3));
    }

    @Test
    void searchForPartialHyphenatedAndNumberedNameWithoutHyphenMatchesEntry3() {
        String query = proteinName("14 3");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_3));
    }

    @Test
    void noMatchForPartialIncorrectHyphenatedAndNumberedNameMatches0Entries() {
        String query = proteinName("14-1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void searchForNameWithParenthesisMatchesEntry4() {
        String query = proteinName(NAME_4);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_4));
    }

    @Test
    void partialSearchUsingNameInBetweenParenthesisWithMatchesEntry4() {
        String query = proteinName("HAE1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_4));
    }

    @Test
    void searchForNameWithSingleQuoteMatchesEntry5() {
        String query = proteinName(NAME_5);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_5));
    }

    @Test
    void partialSearchForNameWithoutSingleQuoteMatchesEntry5() {
        String query = proteinName("B-delta");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_5));
    }

    @Test
    void searchForNameWithChemicalSymbolUsingMatchesEntry6() {
        String query = proteinName(NAME_6);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_6));
    }

    @Test
    void partialSearchForNameWithChemicalSymbolUsingSubstringOfChemicalSymbolMatchesEntry6() {
        String query = proteinName("NADP");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ID_6));
    }

    private String proteinName(String value) {
        return QueryBuilder.query(UniParcField.Search.protein.name(), value);
    }
}
