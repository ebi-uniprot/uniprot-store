package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
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
import org.uniprot.core.uniprot.comment.CommentType;

public class CCWebResourceSearchIT {
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
				"CC   -!- WEB RESOURCE: Name=Wikipedia; Note=Aspartate carbamoyltransferase\n" + 
				"CC       entry;\n" + 
				"CC       URL=\"https://en.wikipedia.org/wiki/Aspartate_carbamoyltransferase\";");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- WEB RESOURCE: Name=PTPRCbase; Note=PTPRC mutation db;\n" + 
				"CC       URL=\"http://structure.bmc.lu.se/idbase/PTPRCbase/\";");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}
	@Test
	public void findTwo() {
		String query = comments(CommentType.WEBRESOURCE, "*");

		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
	}
	@Test
	public void findOne() {
		String query = comments(CommentType.WEBRESOURCE, "carbamoyltransferase");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX3)));
	}
}
