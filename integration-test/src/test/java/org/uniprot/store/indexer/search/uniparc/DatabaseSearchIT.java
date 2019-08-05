package org.uniprot.store.indexer.search.uniparc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.uniparc.UniParcDatabaseType;
import org.uniprot.core.xml.jaxb.uniparc.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniParcField;

/**
 * Tests the search capabilities of the {@link UniParcQueryBuilder} when it comes to searching for UniParc entries
 * that reference database source accessions
 */
public class DatabaseSearchIT {
    @ClassRule
    public static UniParcSearchEngine searchEngine = new UniParcSearchEngine();

    private static final String ID_1 = "UPI0000000001";
    private static final String ID_2 = "UPI0000000002";
    private static final String ID_3 = "UPI0000000003";
    private static final String ACC_P47986 = "P47986";
    private static final String ACC_P47986_1 = "P47986-1";
    private static final String ACC_P47988 = "P47988";
    private static final String ACC_ENSP00000226587 = "ENSP00000226587";
    private static final String ACC_NC_000004_1185_0 = "NC_000004_1185_0";

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        //Entry 1
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_1);
            entry.getDbReference().clear();
            entry.getDbReference().add(TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), ACC_P47986, "Y"));
            searchEngine.indexEntry(entry);
        }

        //Entry 2
        {
            Entry entry = TestUtils.createDefaultUniParcEntry();
            entry.setAccession(ID_2);
            entry.getDbReference().clear();
            entry.getDbReference().add(TestUtils.createXref(UniParcDatabaseType.ENSEMBL_VERTEBRATE.getName(), ACC_ENSP00000226587, "Y"));
            entry.getDbReference().add(TestUtils.createXref(UniParcDatabaseType.SWISSPROT_VARSPLIC.getName(), ACC_P47986_1, "Y"));
            searchEngine.indexEntry(entry);
        }

        //Entry 3
        {
        	   Entry entry = TestUtils.createDefaultUniParcEntry();
               entry.setAccession(ID_3);
               entry.getDbReference().clear();
               entry.getDbReference().add(TestUtils.createXref(UniParcDatabaseType.TREMBL.getName(), ACC_P47988, "N"));        
               entry.getDbReference().add(TestUtils.createXref(UniParcDatabaseType.REFSEQ.getName(), ACC_NC_000004_1185_0, "Y"));           
               searchEngine.indexEntry(entry);

        }

        searchEngine.printIndexContents();
    }
    @Test
    public void testAllUniProt() throws Exception{
    	 String query =uniprot("*");
         
         QueryResponse queryResponse =
                 searchEngine.getQueryResponse(query);
         List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
         
         Assert.assertEquals(2, retrievedAccessions.size());
         assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_3));
    }
    @Test
    public void testAllUniProtIsoform() throws Exception{
    	 String query =isoform("*");
         
         QueryResponse queryResponse =
                 searchEngine.getQueryResponse(query);
         List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
         
         Assert.assertEquals(3, retrievedAccessions.size());
         assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2, ID_3));
    }
    
    @Test
    public void testActiveTrembl() throws Exception{
    	  String query =active(UniParcDatabaseType.TREMBL.getName());
          
          QueryResponse queryResponse =
                  searchEngine.getQueryResponse(query);
          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
          System.out.println(retrievedAccessions);
          Assert.assertEquals(1, retrievedAccessions.size());
          assertThat(retrievedAccessions, containsInAnyOrder(ID_1));
    }
    
    @Test
    public void testDBUniProt() throws Exception{
    	  String query =database(UniParcDatabaseType.TREMBL.getName());
          
          QueryResponse queryResponse =
                  searchEngine.getQueryResponse(query);
          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
          
          Assert.assertEquals(2, retrievedAccessions.size());
          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_3));
    }
    
    @Test
    public void testDBREFSEQ() throws Exception{
    	  String query =database(UniParcDatabaseType.REFSEQ.getName());
          
          QueryResponse queryResponse =
                  searchEngine.getQueryResponse(query);
          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);

          Assert.assertEquals(1, retrievedAccessions.size());
          assertThat(retrievedAccessions, containsInAnyOrder(ID_3));
    }
    
    private String active(String dbname) {
    	return QueryBuilder.query(UniParcField.Search.active.name(),dbname);
    }
    private String database(String dbname) {
    	return QueryBuilder.query(UniParcField.Search.database.name(),dbname);
    }
    public String uniprot(String acc) {
    	return QueryBuilder.query(UniParcField.Search.accession.name(),acc);
    }
    public String isoform(String acc) {
    	return QueryBuilder.query(UniParcField.Search.isoform.name(),acc);
    }
    
    
}