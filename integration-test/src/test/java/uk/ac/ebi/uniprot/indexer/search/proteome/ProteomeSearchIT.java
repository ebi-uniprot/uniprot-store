package uk.ac.ebi.uniprot.indexer.search.proteome;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.solr.client.solrj.beans.DocumentObjectBinder;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import uk.ac.ebi.uniprot.json.parser.proteome.ProteomeJsonConfig;
import uk.ac.ebi.uniprot.search.document.proteome.ProteomeDocument;
import uk.ac.ebi.uniprot.search.field.ProteomeField;
import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.xml.XmlChainIterator;
import uk.ac.ebi.uniprot.xml.jaxb.proteome.Proteome;

public class ProteomeSearchIT {
	 static final String PROTEOME_ROOT_ELEMENT = "proteome";
    @ClassRule
    public static ProteomeSearchEngine searchEngine = new ProteomeSearchEngine();

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        List<String> files = Arrays.asList(
                "it/proteome/pds_sample.xml"
               
        );

        XmlChainIterator<Proteome, Proteome>  chainingIterators =
        		new XmlChainIterator<>(new XmlChainIterator.FileInputStreamIterator(files),
                        Proteome.class, PROTEOME_ROOT_ELEMENT, Function.identity() );
        		
                new XmlChainIterator<>(new XmlChainIterator.FileInputStreamIterator(files),
                        Proteome.class,
                        PROTEOME_ROOT_ELEMENT, Function.identity() );

        while (chainingIterators.hasNext()) {
            Proteome next =
                    chainingIterators.next();
            searchEngine.indexEntry(next);
        }
    }
    
    @Test
    public void searchUPid(){
        String upid = "UP000002199";
        String query = upid(upid);
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).containsValue(upid)); 
    }
    
    @Test
    public void searchAllUPid(){
        String upid = "*";
        String query = upid(upid);
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(10, results.size());
    }
    
    
    @Test
    public void searchListUPid() {
        List<String> upids = Arrays.asList("UP000000798", "UP000001258", "UP000036222" ,"UP000036221");

     
        String query = upid(upids.get(0));
        for (int i = 1; i < upids.size(); i++) {
        	query = QueryBuilder.or(query,  upid(upids.get(i)));
        }
        
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(3, results.size());
        List<String> foundUpids=
        StreamSupport.
        stream( Spliterators.spliteratorUnknownSize(
                results.listIterator(),
                Spliterator.ORDERED
            ),
                 false).map(val->val.getFieldValue("upid"))
                 .map(val -> (String) val)
                 .collect(Collectors.toList());
        
        assertTrue(foundUpids.contains("UP000000798"));
        
        assertTrue(foundUpids.contains("UP000001258"));
        
        assertTrue(foundUpids.contains("UP000036221")); 
        assertFalse(foundUpids.contains("UP000036222")); 
    }
    
  
    @Test
    public void searchTaxId(){
        int taxId=272557;
        String query =taxonomy(taxId);
        
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).containsValue("UP000002518")); 
    }
    
    @Test
    public void searchIsRedundant(){
      
        String query =isRedudant(true);
        
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(3, results.size());
       
        Assert.assertTrue(results.get(0).containsValue("UP000036221")
                ||results.get(1).containsValue("UP000036221")
               || results.get(2).containsValue("UP000036221")); 
     //   UP000000802
    //    UP000066929
     //   UP000036221
    }
 
//    @Test
//    public void searchByGeneAccession(){
//        String query =accession("Q9Y8V6");
//        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
//        SolrDocumentList results = queryResponse.getResults();
//
//        Assert.assertEquals(1, results.size());
//        Assert.assertTrue(results.get(0).containsValue("UP000002518"));
//    }
//
//    @Test
//    public void searchByRelatedGeneAccession(){
//        String query =accession("Q9YCC8");
//        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
//        SolrDocumentList results = queryResponse.getResults();
//
//        Assert.assertEquals(1, results.size());
//        Assert.assertTrue(results.get(0).containsValue("UP000002518"));
//    }
//
//    @Test
//    public void searchByGeneName(){
//        String query =gene("CELE_F29G6.3");
//        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
//        SolrDocumentList results = queryResponse.getResults();
//
//        Assert.assertEquals(1, results.size());
//        Assert.assertTrue(results.get(0).containsValue("UP000001940"));
//    }
//
//    @Test
//    public void searchByRelatedGeneName(){
//        String query =gene("T06F4.2");
//        QueryResponse queryResponse = searchEngine.getQueryResponse(query);
//        SolrDocumentList results = queryResponse.getResults();
//
//        Assert.assertEquals(2, results.size());
//        Assert.assertTrue(results.get(0).containsValue("UP000001940"));
//    }
    
    @Test
    public void fetchAvroObject(){
    	String upid = "UP000000798";
        String query =upid(upid);
        
        QueryResponse queryResponse =
                searchEngine.getQueryResponse(query);

        SolrDocumentList results =
                queryResponse.getResults();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).containsValue("UP000000798")); 
        DocumentObjectBinder binder = new DocumentObjectBinder();
        ProteomeDocument proteomeDoc = binder.getBean(ProteomeDocument.class, results.get(0));
        uk.ac.ebi.uniprot.domain.proteome.ProteomeEntry proteome = toProteome(proteomeDoc);
        assertNotNull(proteome);
        assertEquals("UP000000798", proteome.getId().getValue());
        
    }
    
    uk.ac.ebi.uniprot.domain.proteome.ProteomeEntry toProteome(ProteomeDocument proteomeDoc){
    	try {
    	ObjectMapper objectMapper =  ProteomeJsonConfig.getInstance().getFullObjectMapper();
    	return objectMapper.readValue(proteomeDoc.proteomeStored.array(),  uk.ac.ebi.uniprot.domain.proteome.ProteomeEntry.class);
    	}catch(Exception e) {
    		throw new RuntimeException (e);
    	}
    	
    }
    private String upid(String upid) {
    	return QueryBuilder.query(ProteomeField.Search.upid.name(),upid);
    }
    private String taxonomy(int taxId) {
    	return QueryBuilder.query(ProteomeField.Search.taxonomy_id.name(), ""+taxId);
    }
   
    private String isRedudant(Boolean b) {

    		return QueryBuilder.query(ProteomeField.Search.redundant.name(), b.toString());
    
    }
}

