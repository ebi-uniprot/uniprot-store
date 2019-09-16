package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

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

public class CCCofactorSearchIT {
	 public static final String Q6GZX4 = "Q6GZX4";
	    public static final String Q6GZX3 = "Q6GZX3";
	    public static final String Q6GZY3 = "Q6GZY3";
	    public static final String Q197B6 = "Q197B6";
	    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	    private static final String Q196W5 = "Q196W5";
	    private static final String Q6GZN7 = "Q6GZN7";

	    @RegisterExtension
	    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	    @BeforeAll
	    public static void populateIndexWithTestData() throws IOException {
	        // a test entry object that can be modified and added to index
	        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
	        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

	    
	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- COFACTOR:\n" + 
	                "CC       Name=pyridoxal 5' phosphate; Xref=ChEBI:CHEBI:597326;\n" + 
	                "CC         Evidence={ECO:0000250};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- COFACTOR:\n" + 
	                "CC       Name=pantetheine 4' phosphate; Xref=ChEBI:CHEBI:47942;\n" + 
	                "CC         Evidence={ECO:0000256|PIRSR:PIRSR001111-50};\n" + 
	                "CC       Note=Binds 1 phosphopantetheine covalently.\n" + 
	                "CC       {ECO:0000256|PIRSR:PIRSR001111-50};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- COFACTOR:\n" + 
	                "CC       Name=Mg(2+); Xref=ChEBI:CHEBI:18420;\n" + 
	                "CC         Evidence={ECO:0000256|RuleBase:RU361271};\n" + 
	                "CC       Name=Mn(2+); Xref=ChEBI:CHEBI:29035;\n" + 
	                "CC         Evidence={ECO:0000256|RuleBase:RU361271};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q196W5));
	        entryProxy.updateEntryObject(LineType.CC, "CC   -!- COFACTOR:\n" +
	                "CC       Name=Zn(2+); Xref=ChEBI:CHEBI:29105; Evidence={ECO:0000250};\n" +
	                "CC       Note=Binds 1 zinc ion per subunit. {ECO:0000250};" );
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
	        entryProxy
	                .updateEntryObject(LineType.CC, 
	                        "CC   -!- COFACTOR:\n" +
	                        "CC       Name=FAD; Xref=ChEBI:CHEBI:57692;\n" +
	                        "CC         Evidence={ECO:0000255|PROSITE-ProRule:PRU00654};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
	      

	        searchEngine.printIndexContents();
	    }

	    @Test
	    public void findCofactorWithChebi() {
	    		String query= query(UniProtField.Search.cc_cofactor_chebi, "57692");
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, contains(Q6GZN7));
	    }
	    
	    @Test
	    public void findCofactorWithChebiName() {
	    		String query= query(UniProtField.Search.cc_cofactor_chebi, "Mg(2+)");
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, contains(Q197B6));
	    }
	    
	    @Test
	    public void findCofactorWithChebiName2() {
	    		String query= query(UniProtField.Search.cc_cofactor_chebi, "phosphate");
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, contains(Q6GZX3, Q6GZY3));
	    }
	    @Test
	    public void findCofactorWithChebiNameEvidence() {
	    		String query= query(UniProtField.Search.cc_cofactor_chebi, "phosphate");
	    		String evidence ="ECO_0000256";
	    		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_cofactor_chebi, evidence));
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            System.out.println(retrievedAccessions);
	            assertThat(retrievedAccessions, hasItems( Q6GZY3));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZX3)));
	    }
	    
	    
	    @Test
	    public void findCofactorWithNote() {
	    		String query= query(UniProtField.Search.cc_cofactor_note, "binds");
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, hasItems(Q196W5, Q6GZY3));
	    }
	    
	    @Test
	    public void findCofactorWithNoteEvidence() {
	    		String query= query(UniProtField.Search.cc_cofactor_note, "binds");
	    		String evidence ="ECO_0000250";
	    		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_cofactor_note, evidence));
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, hasItems( Q196W5));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZY3)));
	    }
	    @Test
	    public void findCofactorWithNoteAAEvidence() {
	    		String query= query(UniProtField.Search.cc_cofactor_note, "binds");
	    		String evidence ="automatic";
	    		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_cofactor_note, evidence));
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, hasItems( Q6GZY3));
	    		assertThat(retrievedAccessions, not(hasItem(Q196W5)));
	    }
	    @Test
	    public void findCofactorWithNoteManualEvidence() {
	    		String query= query(UniProtField.Search.cc_cofactor_note, "binds");
	    		String evidence ="manual";
	    		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_cofactor_note, evidence));
	    	
	    		QueryResponse response = searchEngine.getQueryResponse(query);

	            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
	            assertThat(retrievedAccessions, hasItems( Q196W5));
	    		assertThat(retrievedAccessions, not(hasItem(Q6GZY3)));
	    }
}
