package uk.ac.ebi.uniprot.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.*;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.domain.uniprot.comment.CommentType;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.indexer.document.field.QueryBuilder;
import uk.ac.ebi.uniprot.indexer.document.field.UniProtField;


public class CCInteractionSearchIT {
	 public static final String Q6GZX4 = "Q6GZX4";
	    public static final String Q6GZX3 = "Q6GZX3";
	    public static final String Q6GZY3 = "Q6GZY3";
	    public static final String Q197B6 = "Q197B6";
	    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	    @ClassRule
	    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	    @BeforeClass
	    public static void populateIndexWithTestData() throws IOException {
	        // a test entry object that can be modified and added to index
	        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
	        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

	    
	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- INTERACTION:\n" + 
	                "CC       Q8NB12:SMYD1; NbExp=2; IntAct=EBI-1042898, EBI-8463848;");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- SUBUNIT: The carbonic-anhydrase like domain interacts with CNTN1\n" + 
	                "CC       (contactin) (PubMed:20133774). Interacts with PTN\n" + 
	                "CC       (PubMed:16814777). Interaction with PTN promotes formation of\n" + 
	                "CC       homooligomers; oligomerization impairs phosphatase activity (By\n" + 
	                "CC       similarity). {ECO:0000250|UniProtKB:Q62656,\n" + 
	                "CC       ECO:0000269|PubMed:16814777, ECO:0000269|PubMed:20133774}.");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        searchEngine.printIndexContents();
	    }
	
	    @Test
	    public void interactionFindOne() {
	    	String query= query(UniProtField.Search.interactor, "Q8NB12");
		
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems(Q6GZX3));
	    }
	    @Test
	    public void interactionFindOne2() {
	    	String query= query(UniProtField.Search.interactor, "EBI-1042898");
	    	
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems(Q6GZX3));
	    }
	    
	    @Test
	    public void subunitFindOne() {
	    	String query = comments(CommentType.SUBUNIT, "domain");
			String evidence = "ECO_0000269";
			query = QueryBuilder.and(query, commentEvidence(CommentType.SUBUNIT, evidence));
	    	
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems(Q6GZY3));
	    }
	  
}
