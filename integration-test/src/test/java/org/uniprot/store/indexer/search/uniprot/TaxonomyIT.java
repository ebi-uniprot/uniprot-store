package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.OrganismIT.OX_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests whether the taxonomy lineages have been indexed correctly
 * taxonomy lineages Index is based on taxonomy.dat file. See how we load file content at: FileNodeIterable.createNode
 * and how we index at UniprotEntryConverter.setLineageTaxons
 */
public class TaxonomyIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    //Entry 1
    private static final String ACCESSION1 = "Q197F4";
    private static final int ORGANISM_TAX_ID1 = 7787; //Lineage: 7787 --> 7711 --> 33208 --> 2759 --> 1

    //Entry 2
    private static final String ACCESSION2 = "Q197F5";
    private static final int ORGANISM_TAX_ID2 = 100673; //Lineage: 100673 --> 11552 --> 197913 --> 11308 --> 1

    //Entry 3
    private static final String ACCESSION3 = "Q197F6";
    private static final int ORGANISM_TAX_ID3 = 93838; //Lineage: 93838 --> 11320 -->  197911 --> 11308 --> 1

    @RegisterExtension
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, ORGANISM_TAX_ID1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, ORGANISM_TAX_ID2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, ORGANISM_TAX_ID3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    public void noMatchesForNonExistentName() throws Exception {
        String query = taxonName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    public void lineageNameMatchesEntry1() throws Exception {
        String query = taxonName("Tetronarce californica");
        query =QueryBuilder.and(query,taxonName("Pacific electric ray"));
        query =QueryBuilder.and(query,taxonName("Torpedo californica"));
        query =QueryBuilder.and(query,taxonName("TETCF")); //7787
        query =QueryBuilder.and(query,taxonName("Chordata"));
        query =QueryBuilder.and(query,taxonName("9CHOR")); //7711
        query =QueryBuilder.and(query,taxonName("Metazoa"));
        query =QueryBuilder.and(query,taxonName("9METZ")) ;//33208
        query =QueryBuilder.and(query,taxonName("Eukaryota"));
        query =QueryBuilder.and(query,taxonName("9EUKA")) ;//2759
        query =QueryBuilder.and(query,taxonName("root")); //1

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1));
    }

    @Test
    public void lineageNameMatchesEntry2() throws Exception {
        String query = taxonName("Influenza C virus (strain C/Johannesburg/1/1966)\n");
        query =QueryBuilder.and(query,taxonName("INCJH")); //100673
        query =QueryBuilder.and(query,taxonName("Influenza C virus")) ;//11552
        query =QueryBuilder.and(query,taxonName("Gammainfluenzavirus")); //197913
        query =QueryBuilder.and(query,taxonName("Orthomyxoviridae"));
        query =QueryBuilder.and(query,taxonName("9ORTO")) ;//11308
        query =QueryBuilder.and(query,taxonName("root")); //1

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2));
    }

    @Test
    public void lineageNameMatchesEntry3() throws Exception {
        String query = taxonName("Influenza A virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)");
        query =QueryBuilder.and(query, taxonName("I96A0"));
        query =QueryBuilder.and(query,taxonName("Influenza A virus"));
        query =QueryBuilder.and(query,taxonName("9INFA"));
        query =QueryBuilder.and(query,taxonName("Alphainfluenzavirus")) ;
        query =QueryBuilder.and(query,taxonName("Orthomyxoviridae"));
        query =QueryBuilder.and(query,taxonName("9ORTO")) ;
        query =QueryBuilder.and(query,taxonName("root")); //1

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION3));
    }



    @Test
    public void commonLineageNameMatchesEntry2And3() throws Exception {
        String query = taxonName("Orthomyxoviridae");
        query =QueryBuilder.and(query, taxonName("9ORTO")); //ID: 11308

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2, ACCESSION3));
    }

    @Test
    public void partialCommonLineageNameMatchesEntry2And3() throws Exception {
        String query = taxonName("influenza");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2, ACCESSION3));
    }

    @Test
    public void lineageIdMatchesEntry1() throws Exception {
        String query = taxonID(7787);
        query =QueryBuilder.and(query, taxonID(7711));
        query =QueryBuilder.and(query, taxonID(33208));
        query =QueryBuilder.and(query, taxonID(2759));
        query =QueryBuilder.and(query, taxonID(1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1));
    }

    @Test
    public void lineageIdMatchesEntry2() throws Exception {
        String query = taxonID(100673);
        query =QueryBuilder.and(query, taxonID(11552));
        query =QueryBuilder.and(query, taxonID(197913));
        query =QueryBuilder.and(query, taxonID(11308));
        query =QueryBuilder.and(query, taxonID(1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2));
    }

    @Test
    public void lineageIdMatchesEntry3() throws Exception {
        String query = taxonID(93838);
        query =QueryBuilder.and(query, taxonID(11320));
        query =QueryBuilder.and(query, taxonID(197911));
        query =QueryBuilder.and(query, taxonID(11308));
        query =QueryBuilder.and(query, taxonID(1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION3));
    }


    @Test
    public void commonLineageIDMatchesEntry2And3() throws Exception {
        String query = taxonID(11308);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION2, ACCESSION3));
    }
    String taxonName(String value) {
    	return query(UniProtField.Search.taxonomy_name, value);
    }
    public static String taxonID(int taxonomy) {
        return query(UniProtField.Search.taxonomy_id, String.valueOf(taxonomy));
    }
}