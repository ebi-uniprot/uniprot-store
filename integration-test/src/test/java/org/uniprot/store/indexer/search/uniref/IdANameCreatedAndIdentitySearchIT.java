package org.uniprot.store.indexer.search.uniref;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.uniprot.store.search.field.QueryBuilder.rangeQuery;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.xml.jaxb.uniref.Entry;
import org.uniprot.core.xml.uniprot.XmlConverterHelper;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniRefField;


/**
 *
 * @author jluo
 * @date: 19 Aug 2019
 *
*/

public class IdANameCreatedAndIdentitySearchIT {
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
	            LocalDate localDate = LocalDate.of(2015, 9, 12);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 2
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_2, NAME_2);
	            LocalDate localDate = LocalDate.of(2015, 9, 12);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 3
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_3, NAME_3);
	            LocalDate localDate = LocalDate.of(2015, 10, 9);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 4
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_4, NAME_4);
	            LocalDate localDate = LocalDate.of(2015, 8, 9);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 5
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_5, NAME_5);
	            LocalDate localDate = LocalDate.of(2016, 2, 9);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }
	        //Entry 6
	        {
	            Entry entry = TestUtils.createSkeletonEntry(ID_6, NAME_6);
	            LocalDate localDate = LocalDate.of(2016, 3, 9);
	            entry.setUpdated(XmlConverterHelper.dateToXml(localDate));
	            searchEngine.indexEntry(entry);
	        }	      
	        searchEngine.printIndexContents();
	    }
	    @Test
	    public void uniref100Id() {
	    	String  query =idQuery(ID_1);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1));
	    }
	
	    
	    @Test
	    public void uniref90Id() {
	    	String  query =idQuery(ID_3);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_3));
	    }
	    
	    @Test
	    public void uniref50Id() {
	    	String  query =idQuery(ID_5);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(1, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_5));
	    }
	    @Test
	    public void unirefNoId() {
	    	String  query =idQuery("UniRef100_A0A002");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());

	    }
	    @Test
	    public void unirefNoId2() {
	    	String  query =idQuery("UniRef10_A0A007");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());

	    }
	    @Test
	    public void unirefName() {
	    	String  query =nameQuery("MoeK5");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_3));
	    }
	    
	    @Test
	    public void unirefName2() {
	    	String  query =nameQuery("Transposase");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(3, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_2, ID_4,  ID_6));
	    }
	    @Test
	    public void unirefNameNo() {
	    	String  query =nameQuery("Transposa");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());

	    }
	    
	    @Test
	    public void unirefIdentity100() {
	    	String  query =identityQuery("1.0");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2));
	    }
	    @Test
	    public void unirefIdentity90() {
	    	String  query =identityQuery("0.9");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_3, ID_4));
	    }
	    
	    @Test
	    public void unirefIdentity50() {
	    	String  query =identityQuery("0.5");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_5, ID_6));
	    }
	    
	    @Test
	    public void unirefIdentity80() {
	    	String  query =identityQuery("0.8");
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(0, retrievedAccessions.size());

	    }
	    
	    @Test
	    public void unirefCreatedSingle() {
	    	 LocalDate start = LocalDate.of(2015, 9, 11);
	    	 LocalDate end = LocalDate.of(2015, 9, 12);
	    	 String query = rangeQuery(UniRefField.Search.created.name(), start, end);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(2, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2));

	    }
	    @Test
	    public void unirefCreatedRange() {
	    	 LocalDate start = LocalDate.of(2015, 8, 8);
	    	 LocalDate end = LocalDate.of(2015, 10, 9);
	    	 String query = rangeQuery(UniRefField.Search.created.name(), start, end);
	    	  QueryResponse queryResponse =
	                  searchEngine.getQueryResponse(query);
	          List<String> retrievedAccessions = searchEngine.getIdentifiers(queryResponse);
	          
	          Assert.assertEquals(4, retrievedAccessions.size());
	          assertThat(retrievedAccessions, containsInAnyOrder(ID_1, ID_2, ID_3, ID_4));

	    }
	    private String idQuery(String  id) {
	    	return QueryBuilder.query(UniRefField.Search.id.name(),id);
	    }
	    private String nameQuery(String  name) {
	    	return QueryBuilder.query(UniRefField.Search.name.name(),name);
	    }
	    private String identityQuery(String  identity) {
	    	return QueryBuilder.query(UniRefField.Search.identity.name(), identity);
	    }
}

