package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
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
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.store.search.field.QueryBuilder;


public class CCRnaEditingSearchIT {
	public static final String Q6GZX4 = "Q6GZX4";
	public static final String Q6GZX3 = "Q6GZX3";
	public static final String Q6GZY3 = "Q6GZY3";
	public static final String Q197B6 = "Q197B6";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";

	@RegisterExtension
	public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	@BeforeAll
	public static void populateIndexWithTestData() throws IOException {
		// a test entry object that can be modified and added to index
		InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
		UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- RNA EDITING: Modified_positions=21 {ECO:0000256|PubMed:17714582};\n" + 
				"CC       Note=Editing at position 21 is more efficient in\n" + 
				"CC       photosynthetically active tissue.;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- RNA EDITING: Modified_positions=113 {ECO:0000269|PubMed:17714582},\n" + 
				"CC       158 {ECO:0000269|PubMed:17714582}, 184\n" + 
				"CC       {ECO:0000269|PubMed:17714582}, 189 {ECO:0000269|PubMed:17714582},\n" + 
				"CC       667 {ECO:0000269|PubMed:17714582}; Note=Editing at positions 113\n" + 
				"CC       and 158 is more efficient in photosynthetically active tissue.;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}
	@Test
	public void shouldFindTwoEntryQuery() {
		String query = comments(CommentType.RNA_EDITING, "active");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
	}

	@Test
	public void shouldFindTwoEntryQueryEvidence() {
		String query = comments(CommentType.RNA_EDITING, "active");
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, commentEvidence(CommentType.RNA_EDITING, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX3));
	}

	@Test
	public void shouldFindNoneEntryQueryEvidence() {
		String query = comments(CommentType.RNA_EDITING, "active");
		String evidence = "ECO_0000255";
		query = QueryBuilder.and(query, commentEvidence(CommentType.RNA_EDITING, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, empty());
	}
}
