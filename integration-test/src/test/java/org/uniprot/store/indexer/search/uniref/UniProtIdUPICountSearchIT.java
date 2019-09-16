package org.uniprot.store.indexer.search.uniref;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.jaxb.uniref.MemberType;
import org.uniprot.core.xml.jaxb.uniref.PropertyType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniRefField;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *
 * @author jluo
 * @date: 19 Aug 2019
 *
*/

public class UniProtIdUPICountSearchIT {
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
	    private static final String ACCESSION_1 = "A0A007";
	    private static final String ACCESSION_2 = "A0A009DWI3";
	    private static final String ACCESSION_4 = "A0A009DWL0";
	    private static final String ACCESSION_5 = "A0A009E3M2";
	    private static final String ACCESSION_6 = "A0A009EC87";
	    
	    private static final String UNIPROTID_1 = "A0A007_HUMAN";
	    private static final String UNIPROTID_2 = "A0A009DWI3_HUMAN";
	    private static final String UNIPROTID_4 = "A0A009DWL0_HUMAN";
	    private static final String UNIPROTID_5 = "A0A009E3M2_HUMAN";
	    private static final String UNIPROTID_6 = "A0A009EC87_HUMAN";
	    
	    private static final String UPI_1 = "UPI0000000001";
	    private static final String UPI_2 = "UPI0000000002";
	    private static final String UPI_3 = "UPI0000000003";
	    
	    
	    @RegisterExtension
	    public static UniRefSearchEngine searchEngine = new UniRefSearchEngine();
	    
	    @BeforeAll
	    public static void populateIndexWithTestData() throws IOException {
	        //Entry 1
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_1, NAME_1);
	            
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYET";
	            
	            MemberType repMember = TestUtils.createRepresentativeMember("UniProtKB ID",UNIPROTID_1 , createProperty(ACCESSION_1, UPI_1), sequence);
	            entry.setRepresentativeMember(repMember);
	            searchEngine.indexEntry(entry);
	            
	        }
	        //Entry 2
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_2, NAME_2);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYETGDF";
	            MemberType repMember = TestUtils.createRepresentativeMember("UniProtKB ID",UNIPROTID_2 , createUniProtAccProperty(ACCESSION_2), sequence);
	            entry.setRepresentativeMember(repMember);
	            searchEngine.indexEntry(entry);
	        }
	        
	        //Entry 3
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_3, NAME_3);
	            String sequence = "MLKHSATWVTPLDELKALTVLNLEPNLTHKIFEQRIALLRLGKQDVVIYETGDF";
	            MemberType repMember = TestUtils.createRepresentativeMember("UniParc ID",UPI_3 , createUniProtAccProperty(ACCESSION_2), sequence);
	            entry.setRepresentativeMember(repMember);
	            searchEngine.indexEntry(entry);
	        }

	        //Entry 4
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_4, NAME_4);
	            MemberType member = TestUtils.createMember("UniProtKB ID",UNIPROTID_4 , createProperty(ACCESSION_4, UPI_3) );
	            entry.getMember().add(member);
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 5
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_5, NAME_5);
	            MemberType member = TestUtils.createMember("UniProtKB ID",UNIPROTID_5 , createProperty(ACCESSION_5, UPI_1) );
	            entry.getMember().add(member);
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 6
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_6, NAME_6);
	            MemberType member1 = TestUtils.createMember("UniProtKB ID",UNIPROTID_1 , createUniProtAccProperty(ACCESSION_1) );
	            entry.getMember().add(member1);
	            MemberType member = TestUtils.createMember("UniProtKB ID",UNIPROTID_4 , createUniProtAccProperty(ACCESSION_4) );
	            entry.getMember().add(member);
	            MemberType member2 = TestUtils.createMember("UniProtKB ID",UNIPROTID_6 , createProperty(ACCESSION_6, UPI_2) );
	            entry.getMember().add(member2);
	            searchEngine.indexEntry(entry);
	        }	      
	        searchEngine.printIndexContents();
	    }
	    
	    @Test
	    public void testAccession() {
	    	String  query =uniprotIdQuery(ACCESSION_1);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_6));
	    }
	    @Test
	    public void testAccession5() {
	    	String  query =uniprotIdQuery(ACCESSION_5);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_5));
	    }
	    
	    @Test
	    public void testUniProtID() {
	    	String  query =uniprotIdQuery(UNIPROTID_2);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_2));
	    }
	    @Test
	    public void testUniProtI2D() {
	    	String  query =uniprotIdQuery(UNIPROTID_4);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_4, ID_6));
	    }
	    
	    
	    @Test
	    public void testUPI1() {
	    	String  query =upiQuery(UPI_1);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_5));
	    }
	    
	    @Test
	    public void testUPI2() {
	    	String  query =upiQuery(UPI_2);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_6));
	    }
	    @Test
	    public void testUPI3() {
	    	String  query =upiQuery(UPI_3);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_3, ID_4));
	    }
	    @Test
	    public void testCount1() {
	    	String  query =countQuery(1);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(3, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2, ID_3));
	    }
	    
	    @Test
	    public void testCount2() {
	    	String  query =countQuery(2);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_4, ID_5));
	    }
	    
	    @Test
	    public void testCount4() {
	    	String  query =countQuery(4);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_6));
	    }
	    
	    @Test
	    public void testCount24() {
	    	String  query =countQuery(2,4);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          assertEquals(3, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_4, ID_5, ID_6));
	    }
	    private String uniprotIdQuery(String  uniprotId) {
	    	return QueryBuilder.query(UniRefField.Search.uniprot_id.name(), uniprotId);
	    }
	    
	    private String upiQuery(String  upi) {
	    	return QueryBuilder.query(UniRefField.Search.upi.name(), upi);
	    }
	    
	    static List<PropertyType> createUniProtAccProperty(String accession){
	    	return Arrays.asList(TestUtils.createProperty("UniProtKB accession", accession));
	    }
	    static List<PropertyType> createProperty(String accession, String upi){
	    	return Arrays.asList(TestUtils.createProperty("UniProtKB accession", accession),
	    			TestUtils.createProperty("UniParc ID", upi)
	    			);
	    }
	    String countQuery(int count) {
	    	return QueryBuilder.query(UniRefField.Search.count.name(),""+count);
	    }
	    String countQuery(int start, int end) {
	    	return QueryBuilder.rangeQuery(UniRefField.Search.count.name(), start, end);
	    }
}

