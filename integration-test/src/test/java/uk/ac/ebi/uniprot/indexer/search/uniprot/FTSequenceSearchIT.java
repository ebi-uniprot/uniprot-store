package uk.ac.ebi.uniprot.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import uk.ac.ebi.uniprot.domain.uniprot.feature.FeatureType;
import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.search.field.QueryBuilder;
import uk.ac.ebi.uniprot.search.field.UniProtField;

public class FTSequenceSearchIT {

	public static final String Q6GZX4 = "Q6GZX4";
	public static final String Q197B1 = "Q197B1";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	private static final String Q12345 = "Q12345";
	private static final String Q6GZN7 = "Q6GZN7";
	private static final String Q6V4H0 = "Q6V4H0";
	@ClassRule
	public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	@BeforeClass
	public static void populateIndexWithTestData() throws IOException {
		// a test entry object that can be modified and added to index
		InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
		UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   VAR_SEQ     234    249       Missing (in isoform 3). {ECO:0000305}.\n"
						+ "FT                                /FTId=VSP_055167.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   VARIANT     177    177       R -> Q (in a colorectal cancer sample;\n"
						+ "FT                                somatic mutation; dbSNP:rs374122292).\n"
						+ "FT                                {ECO:0000269|PubMed:16959974}.\n"
						+ "FT                                /FTId=VAR_035897.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   NON_STD      92     92       Selenocysteine. {ECO:0000250}.\n" +
				"FT   NON_TER       1      1       {ECO:0000303|PubMed:17963685}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   NON_CONS     15     16       {ECO:0000305}.\n" +
				"FT   CONFLICT    758    758       P -> A (in Ref. 1; CAA57732).\n" + 
				"FT                                {ECO:0000305}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   UNSURE       44     44       {ECO:0000269|Ref.1}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		searchEngine.printIndexContents();
	}

	@Test
	public void varSeqFindEntryWithEvidenceLength() {
		String query= features(FeatureType.VAR_SEQ, "isoform");
		query = QueryBuilder.and(query, featureLength(FeatureType.VAR_SEQ, 10, 20));
		String evidence = "ECO_0000305";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.VAR_SEQ, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX4));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}

	@Test
	public void variantFindEntryWithEvidenceLength() {
		String query= features(FeatureType.VARIANT, "colorectal");
		query = QueryBuilder.and(query, featureLength(FeatureType.VARIANT, 1, 1));
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.VARIANT, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q197B1));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}

	@Test
	public void variantsFindEntryWithLengthAndEvidence() {
		String query= query(UniProtField.Search.ft_variants, "colorectal");
		query = QueryBuilder.and(query, QueryBuilder.rangeQuery(UniProtField.Search.ftlen_variants.name(), 1, 21));
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, query(UniProtField.Search.ftev_variants, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q197B1));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}

	@Test
	public void nonStdFindEntryWithEvidenceLength() {
		String query= features(FeatureType.NON_STD, "selenocysteine");
		query = QueryBuilder.and(query, featureLength(FeatureType.NON_STD, 1, 1));
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_STD, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	
	@Test
	public void nonTerFindEntryWithEvidenceLength() {
		String query= features(FeatureType.NON_TER, "*");
		query = QueryBuilder.and(query, featureLength(FeatureType.NON_TER, 1, 1));
		String evidence = "ECO_0000303";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_TER, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	public void nonConsFindEntryWithEvidenceLength() {
		String query= features(FeatureType.NON_CONS, "*");
		query = QueryBuilder.and(query, featureLength(FeatureType.NON_CONS, 1, 2));
		String evidence = "ECO_0000305";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_CONS, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	public void conflictFindEntryWithEvidenceLength() {
		String query= features(FeatureType.CONFLICT, "*");
		query = QueryBuilder.and(query, featureLength(FeatureType.CONFLICT, 1, 2));
		String evidence = "ECO_0000305";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.CONFLICT, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	public void unsureFindEntryWithEvidenceLength() {
		String query= features(FeatureType.UNSURE, "*");
		query = QueryBuilder.and(query, featureLength(FeatureType.UNSURE, 1, 2));
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.UNSURE, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	public void positionFindEntryWithLengthAndEvidence() {
		String query= query(UniProtField.Search.ft_positional, "colorectal");
		query = QueryBuilder.and(query, QueryBuilder.rangeQuery(UniProtField.Search.ftlen_positional.name(), 1, 21));
		String evidence = "ECO_0000269";
		query = QueryBuilder.and(query, query(UniProtField.Search.ftev_positional, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q197B1));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
}
