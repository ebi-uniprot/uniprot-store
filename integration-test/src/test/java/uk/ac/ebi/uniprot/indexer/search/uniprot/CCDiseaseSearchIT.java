package uk.ac.ebi.uniprot.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
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


public class CCDiseaseSearchIT {
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
				"CC   -!- DISEASE: Rickets vitamin D-dependent 1B (VDDR1B) [MIM:600081]: An\n"
						+ "CC       autosomal recessive disorder caused by a selective deficiency of\n"
						+ "CC       the active form of vitamin D (1,25-dihydroxyvitamin D3) and\n"
						+ "CC       resulting in defective bone mineralization and clinical features\n"
						+ "CC       of rickets. The patients sera have low calcium concentrations, low\n"
						+ "CC       phosphate concentrations, elevated alkaline phosphatase activity\n"
						+ "CC       and low levels of 25-hydroxyvitamin D.\n"
						+ "CC       {ECO:0000269|PubMed:15128933, ECO:0000269|PubMed:25942481}.\n"
						+ "CC       Note=The disease is caused by mutations affecting the gene\n"
						+ "CC       represented in this entry.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- DISEASE: Rheumatoid arthritis systemic juvenile (RASJ)\n"
						+ "CC       [MIM:604302]: An inflammatory articular disorder with systemic-\n"
						+ "CC       onset beginning before the age of 16. It represents a subgroup of\n"
						+ "CC       juvenile arthritis associated with severe extraarticular features\n"
						+ "CC       and occasionally fatal complications. During active phases of the\n"
						+ "CC       disorder, patients display a typical daily spiking fever, an\n"
						+ "CC       evanescent macular rash, lymphadenopathy, hepatosplenomegaly,\n"
						+ "CC       serositis, myalgia and arthritis. {ECO:0000269|PubMed:25220867}.\n"
						+ "CC       Note=The gene represented in this entry may be involved in disease\n"
						+ "CC       pathogenesis.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}

	@Test
	public void shouldFindTwoEntryQuery() {
		String query = comments(CommentType.DISEASE, "active");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
	}

	@Test
	public void shouldFindTwoEntryQueryEvidence() {
		String query = comments(CommentType.DISEASE, "active");
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, commentEvidence(CommentType.DISEASE, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
	}

	@Test
	public void shouldFindNoneEntryQueryEvidence() {
		String query = comments(CommentType.DISEASE, "active");
		String evidence = "ECO_0000255";
		query = QueryBuilder.and(query, commentEvidence(CommentType.DISEASE, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, empty());
	}

}
