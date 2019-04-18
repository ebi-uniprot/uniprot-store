package uk.ac.ebi.uniprot.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
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

public class FTSubcellLocationSearchIT {
	public static final String Q6GZX4 = "Q6GZX4";
	public static final String Q197B1 = "Q197B1";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	private static final String Q12345 = "Q12345";
	private static final String Q6GZN7 = "Q6GZN7";
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
				"FT   TRANSMEM    580    604       Helical. {ECO:0000255}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   TRANSMEM    705    725       Helical. {ECO:0000255}.\n" + 
				"FT   TOPO_DOM    726   1070       Cytoplasmic. {ECO:0000255}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   INTRAMEM    510    547       Helical. {ECO:0000250|UniProtKB:F1RAX4}.\n" + 
				"FT   TOPO_DOM    548    667       Cytoplasmic.\n" + 
				"FT                                {ECO:0000250|UniProtKB:F1RAX4}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   MUTAGEN      15     15       T->V: No effect on molecular weight; when\n" + 
				"FT                                associated with V-109 and V-116.\n" + 
				"FT                                {ECO:0000269|PubMed:16956885}.\n" +
				"FT   HELIX         4     13       {ECO:0000244|PDB:4QOB}.\n" + 
				"FT   HELIX        17     27       {ECO:0000244|PDB:4QOB}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
		searchEngine.printIndexContents();
	}

	@Test
	public void transmemFindEntryWithEvidence() {
		String query = features(FeatureType.TRANSMEM, "Helical");
	//	query = QueryBuilder.and(query, featureLength(FeatureType.BINDING, 1, 2));
		String evidence = "ECO_0000255";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.TRANSMEM, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems(Q6GZX4, Q197B1));
		assertThat(retrievedAccessions, not(hasItem(Q12345)));
	}

	@Test
	public void transmemFindEntryWithEvidenceLength() {
		String query = features(FeatureType.TRANSMEM, "Helical");
		query = QueryBuilder.and(query, featureLength(FeatureType.TRANSMEM, 20, 23));
		String evidence = "ECO_0000255";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.TRANSMEM, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, hasItems( Q197B1));
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	
	@Test
	public void topodomFindTwoEntryWithEvidence() {
		String query = features(FeatureType.TOPO_DOM, "cytoplasmic");
		//	query = QueryBuilder.and(query, featureLength(FeatureType.BINDING, 1, 2));
			String evidence = "ECO_0000255";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.TOPO_DOM, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems( Q197B1 ));
			assertThat(retrievedAccessions, not(hasItem(Q12345)));
	}
	@Test
	public void topodomFindEntryWithEvidenceLength() {
		String query = features(FeatureType.TOPO_DOM, "cytoplasmic");
			query = QueryBuilder.and(query, featureLength(FeatureType.TOPO_DOM, 100, 200));
			String evidence = "ECO_0000250";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.TOPO_DOM, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems( Q12345));
			assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	public void intramemFindTwoEntryWithEvidenceLength() {
		String query = features(FeatureType.INTRAMEM, "helical");
			query = QueryBuilder.and(query, featureLength(FeatureType.INTRAMEM, 30, 40));
			String evidence = "ECO_0000250";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.INTRAMEM, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems( Q12345 ));
			assertThat(retrievedAccessions, not(hasItem( Q197B1)));
	}
	@Test
	public void intramemFindTwoEntryWithEvidenceLengthOutRange() {
		String query = features(FeatureType.INTRAMEM, "helical");
			query = QueryBuilder.and(query, featureLength(FeatureType.INTRAMEM, 40, 50));
			String evidence = "ECO_0000250";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.INTRAMEM, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, empty());
	}
	@Test
	public void intramemFindTwoEntryWithNoEvidenceLength() {
		String query = features(FeatureType.INTRAMEM, "helical");
			query = QueryBuilder.and(query, featureLength(FeatureType.INTRAMEM, 30, 40));
			String evidence = "ECO_0000255";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.INTRAMEM, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, empty());
	}
	@Test
	public void mutagenFindEntryWithEvidenceLength() {
		String query = features(FeatureType.MUTAGEN, "molecular");
			query = QueryBuilder.and(query, featureLength(FeatureType.MUTAGEN, 1, 1));
			String evidence = "ECO_0000269";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.MUTAGEN, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems( Q6GZN7 ));
			assertThat(retrievedAccessions, not(hasItem( Q197B1)));
	}
	@Test
	public void helixFindEntryWithEvidenceLength() {
		String query = features(FeatureType.HELIX, "*");
			query = QueryBuilder.and(query, featureLength(FeatureType.HELIX, 5, 11));
			String evidence = "ECO_0000244";
			query = QueryBuilder.and(query, featureEvidence(FeatureType.HELIX, evidence));
			QueryResponse response = searchEngine.getQueryResponse(query);

			List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
			System.out.println(retrievedAccessions);
			assertThat(retrievedAccessions, hasItems( Q6GZN7 ));
			assertThat(retrievedAccessions, not(hasItem( Q197B1)));
	}
}
