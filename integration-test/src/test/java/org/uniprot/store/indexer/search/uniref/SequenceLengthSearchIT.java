package org.uniprot.store.indexer.search.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniRefField;

/**
 *
 * @author jluo
 * @date: 19 Aug 2019
 *
*/

public class SequenceLengthSearchIT {
	  private static final String ID_1 = "UniRef100_A0A007";
	    private static final String ID_2 = "UniRef100_A0A009DWI3";
	    private static final String ID_3 = "UniRef90_A0A007";
	    private static final String ID_4 = "UniRef90_A0A009DWL0";
	    private static final String ID_5 = "UniRef50_A0A009E3M2";
	    private static final String ID_6 = "UniRef50_A0A009EC87";
	    private static final String NAME_1 = "Cluster: MoeK5";
	    private static final String NAME_2 = "Cluster: Transposase DDE domain protein (Fragment)";
	    private static final String NAME_3 = "Cluster: MoeK5";
	    private static final String NAME_4 = "Cluster: Putative iSRSO8-transposase orfB protein";
	    private static final String NAME_5 = "Cluster: Glycosyl transferases group 1 family protein (Fragment)";
	    private static final String NAME_6 = "Cluster: Transposase domain protein";
	    
	    @ClassRule
	    public static UniRefSearchEngine searchEngine = new UniRefSearchEngine();
	    
	    @BeforeClass
	    public static void populateIndexWithTestData() throws IOException {
	        //Entry 1
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_1, NAME_1);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYET";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	            
	        }
	        //Entry 2
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_2, NAME_2);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYETGDF";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 3
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_3, NAME_3);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYET";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 4
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_4, NAME_4);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYETAFDASFGDAFAFDSAFASDFADSFA";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 5
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_5, NAME_5);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYET";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 6
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_6, NAME_6);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYETDFSDSDF";
	            entry.getRepresentativeMember().setSequence(TestUtils.createSequence(sequence));
	            searchEngine.indexEntry(entry);
	        }	      
	        searchEngine.printIndexContents();
	    }
	    @Test 
	    public void exactLength() {
	    	int length=51;
	    	String query =lengthQuery(length);
	    	 QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(3, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_3, ID_5));
	    }
	    
	    @Test 
	    public void exactLength2() {
	    	int length=52;
	    	String query =lengthQuery(length);
	    	 QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());
	        
	    }
	    
	    @Test 
	    public void rangeLength() {
	    	int start =50;
	    	int end = 60;
	    	String query =lengthQuery(start, end);
	    	 QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(5, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2, ID_3, ID_5, ID_6));
	    }
	    
	    @Test 
	    public void rangeLength2() {
	    	int start =45;
	    	int end = 50;
	    	String query =lengthQuery(start, end);
	    	 QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());

	    }
	    
	    
	    String lengthQuery(int length) {
	    	return QueryBuilder.query(UniRefField.Search.length.name(),""+length);
	    }
	    String lengthQuery(int start, int end) {
	    	return QueryBuilder.rangeQuery(UniRefField.Search.length.name(), start, end);
	    }
}

