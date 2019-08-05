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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.store.search.field.QueryBuilder;


public class CCMassSpectrumSearchIT {
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
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- MASS SPECTROMETRY: Mass=11329.741; Mass_error=0.0057;\n" + 
				"CC       Method=Electrospray; Range=1-108;\n" + 
				"CC       Evidence={ECO:0000269|PubMed:20586483};");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- MASS SPECTROMETRY: Mass=24166.0; Mass_error=1.2; Method=MALDI;\n" + 
				"CC       Range=35-268; Evidence={ECO:0000256|PubMed:15578758};");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}
	@Test
	public void shouldFindTwoEntryQuery() {
		String query = comments(CommentType.MASS_SPECTROMETRY, "*");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
	}

	@Test
	public void shouldFindOneEntryQueryEvidence() {
		String query = comments(CommentType.MASS_SPECTROMETRY, "*");
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, commentEvidence(CommentType.MASS_SPECTROMETRY, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX4));
	}

	@Test
	public void shouldFindNoneEntryQueryEvidence() {
		String query = comments(CommentType.MASS_SPECTROMETRY, "*");
		String evidence = "ECO_0000255";
		query = QueryBuilder.and(query, commentEvidence(CommentType.MASS_SPECTROMETRY, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, empty());
	}
}
