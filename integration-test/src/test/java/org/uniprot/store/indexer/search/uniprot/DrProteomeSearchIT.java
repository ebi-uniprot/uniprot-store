package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.UniProtField;

public class DrProteomeSearchIT {
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

	      entryProxy.updateEntryObject(LineType.DR, "DR   Proteomes; UP000005640; Chromosome 14.");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
		 entryProxy.updateEntryObject(LineType.DR, "DR   Proteomes; UP000005640; Chromosome 3.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
		 entryProxy.updateEntryObject(LineType.DR, "DR   Proteomes; UP000000589; Chromosome 14.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}
	@Test
	public void proteomeFindTwoEntryQuery() {
		 String query = query(UniProtField.Search.proteome, "UP000005640");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
		assertThat(retrievedAccessions, not(hasItem(Q197B6)));
	}
	@Test
	public void proteomeComponentFindTwoEntryQuery() {
		 String query = query(UniProtField.Search.proteomecomponent, "Chromosome 14");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q197B6));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX3)));
	}
}
