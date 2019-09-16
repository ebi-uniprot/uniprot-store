package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

/**
 * Verifies if the organism and taxonomy fields are indexed correctly
 * Organism Index is based on taxonomy.dat file. See how we load file content at: FileNodeIterable.createNode
 * and how we index at UniprotEntryConverter.setOrganism
 */
class OrganismIT {
    static final String OX_LINE = "OX   NCBI_TaxID=%d;";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    //Entry 1
    private static final String ACCESSION1 = "Q197F4";
    private static final String SCIENTIFIC_NAME1 = "Sindbis virus";
    private static final String COMMON_NAME1 = "SINV";
    private static final String MNEMONIC1 = "SINDV";
    private static final int TAX_ID1 = 11034;
    //Entry 2
    private static final String ACCESSION2 = "Q197F5";
    private static final String SCIENTIFIC_NAME2 = "Solanum melongena";
    private static final String COMMON_NAME2 = "Eggplant";
    private static final String SYNONYM2 = "Aubergine";
    private static final String MNEMONIC2 = "SOLME";
    private static final int TAX_ID2 = 4111;
    //Entry 3
    private static final String ACCESSION3 = "Q197F6";
    private static final String SCIENTIFIC_NAME3 = "Avian leukosis virus RSA";
    private static final String COMMON_NAME3 = "RSV-SRA";
    private static final String SYNONYM3 = "Rous sarcoma virus (strain Schmidt-Ruppin A)";
    private static final String MNEMONIC3 = "RSVSA";
    private static final int TAX_ID3 = 269446;
    //Entry 4
    private static final String ACCESSION4 = "Q197F7";
    private static final String SCIENTIFIC_NAME4 = "Influenza A virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)";
    private static final String MNEMONIC4 = "I96A0";
    private static final int TAX_ID4 = 93838;
    //Entry 5
    private static final String ACCESSION5 = "Q197F8";
    private static final String SCIENTIFIC_NAME5 = "Influenza C virus (strain C/Johannesburg/1/1966)";
    private static final String MNEMONIC5 = "INCJH";
    private static final int TAX_ID5 = 100673;
    
    //Entry 5
    private static final String ACCESSION6 = "Q197F9";
//    private static final String SCIENTIFIC_NAME6 = "Homo sapiens";
//    private static final String MNEMONIC6 = "HUMAN";
    private static final int TAX_ID6 = 9606;
    
    @RegisterExtension
    static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        //Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID2));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID3));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID4));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 5
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION5));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID5));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        //Entry 6
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION6));
        entryProxy.updateEntryObject(LineType.OX, String.format(OX_LINE, TAX_ID6));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void noMatchesForNonExistentName() throws Exception {
        String query = organismName("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void organismNameFromEntry1MatchesEntry1() throws Exception {
        String query = organismName(SCIENTIFIC_NAME1);
          query =QueryBuilder.and(query, organismName(COMMON_NAME1));
          query =QueryBuilder.and(query, organismName(MNEMONIC1));;

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void organismNameFromEntry2MatchesEntry2() throws Exception {
        String query = organismName(SCIENTIFIC_NAME2);
        query =QueryBuilder.and(query, organismName(COMMON_NAME2));
        query =QueryBuilder.and(query, organismName(SYNONYM2));
        query =QueryBuilder.and(query, organismName(MNEMONIC2));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void organismNameFromEntry3MatchesEntry3() throws Exception {
        String query = organismName(SCIENTIFIC_NAME3);
        query =QueryBuilder.and(query, organismName(COMMON_NAME3));
        query =QueryBuilder.and(query, organismName(SYNONYM3));
        query =QueryBuilder.and(query, organismName(MNEMONIC3));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void organismNameFromEntry4MatchesEntry4() throws Exception {
        String query = organismName(SCIENTIFIC_NAME4);
        query =QueryBuilder.and(query, organismName(MNEMONIC4));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    void organismNameFromEntry5MatchesEntry5() throws Exception {
        String query = organismName(SCIENTIFIC_NAME5);
        query =QueryBuilder.and(query, organismName(MNEMONIC5));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }

    @Test
    void lowerCaseOrganismNameFromEntry1MatchesEntry1() throws Exception {
        String query = organismName(SCIENTIFIC_NAME1.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void partialNameVirusMatches4Entries() throws Exception {
        String query = organismName("virus");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION3, ACCESSION4, ACCESSION5));
    }

    @Test
    void partialHyphenatedNameVirusWillMatchEntries() throws Exception {
        String query = organismName("Schmidt");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void fullHyphenatedNameVirusWillMatchEntries() throws Exception {
        String query = organismName("Schmidt-Ruppin");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void fullMatchWithScapeChars() throws Exception {
        String query = organismName(SYNONYM3);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void partialWordFromForwardSlashSeparatedNameMatchesEntry4() throws Exception {
        String query = organismName("Guangdong");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }
    @Test
    void fullWordFromForwardSlashSeparatedNameMatchesEntry4() throws Exception {
        String query = organismName("A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }


    @Test
    void partialNamePlusWordFromForwardSlashSeparatedNameMatchesEntry5() throws Exception {
        String query = organismName("Influenza Johannesburg");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }


    @Test
    void synonymWithHyphenFromEntry3MatchesEntry3() throws Exception {
        String query = organismName(COMMON_NAME3);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION3));
    }

    @Test
    void taxIDFromEntry1MatchesEntry1() throws Exception {
        String query = taxonID(TAX_ID1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void noMatchesForNonExistentTaxID() throws Exception {
        String query = taxonID(Integer.MAX_VALUE);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void popularOrganismHuman() throws Exception{
    	String query = query(UniProtField.Search.popular_organism, "Human");
    	 QueryResponse response = searchEngine.getQueryResponse(query);

         List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
         assertThat(retrievedAccessions, contains(ACCESSION6));
    	
    }
    @Test
    void popularOrganismNoMouse() throws Exception{
    	String query = query(UniProtField.Search.popular_organism, "Mouse");
    	 QueryResponse response = searchEngine.getQueryResponse(query);

         List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
         assertThat(retrievedAccessions, is(empty()));
    	
    }
    @Test
    void popularOrganismSOLMENotPopular() throws Exception{
    	String query = query(UniProtField.Search.popular_organism, "SOLME");
    	 QueryResponse response = searchEngine.getQueryResponse(query);

         List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
         assertThat(retrievedAccessions, is(empty()));
    	
    }
    @Test
    void otherOrganismSOLME() throws Exception{
    	String query = query(UniProtField.Search.other_organism, "SOLME");
    	 QueryResponse response = searchEngine.getQueryResponse(query);

         List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
         assertThat(retrievedAccessions, contains(ACCESSION2));
    	
    }
    
    @Test
    void otherOrganismHumanNotOther() throws Exception{
    	String query = query(UniProtField.Search.other_organism, "Human");
    	 QueryResponse response = searchEngine.getQueryResponse(query);

         List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
         assertThat(retrievedAccessions, is(empty()));
    	
    }
   private String organismName(String name) {
	   return query (UniProtField.Search.organism_name, name);      
    }
   private static String taxonID(int taxonomy) {
       return query(UniProtField.Search.organism_id, String.valueOf(taxonomy));
   }

}