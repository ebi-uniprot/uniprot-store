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
import org.uniprot.core.uniprot.feature.FeatureType;
import org.uniprot.store.search.field.QueryBuilder;


class FTFamilyDomainSearchIT {
	private static final String Q6GZX4 = "Q6GZX4";
	private static final String Q197B1 = "Q197B1";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	private static final String Q12345 = "Q12345";
	private static final String Q6GZN7 = "Q6GZN7";
	private static final String Q6V4H0 = "Q6V4H0";
	@RegisterExtension
	static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	@BeforeAll
	static void populateIndexWithTestData() throws IOException {
		// a test entry object that can be modified and added to index
		InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
		UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   DOMAIN          1622..2089\n" + 
				"FT                   /note=\"Tyrosine-protein phosphatase\"\n" + 
				"FT                   /evidence=\"ECO:0000259|PROSITE:PS50055\"\n"+
				"FT   DOMAIN          1926..1942\n" + 
				"FT                   /note=\"TYR_PHOSPHATASE_2\"\n" + 
				"FT                   /evidence=\"ECO:0000259|PROSITE:PS50056\"");

		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   COILED          306..334\n" + 
						"FT                   /evidence=\"ECO:0000255\"\n"+
						"FT   COILED          371..395\n" + 
						"FT                   /evidence=\"ECO:0000255\"");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
		entryProxy.updateEntryObject(LineType.FT,
						"FT   COMPBIAS        403..416\n" + 
						"FT                   /note=\"Glu-rich\"\n" + 
						"FT                   /evidence=\"ECO:0000256|HAMAP-Rule:MF_01138\"");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
		entryProxy.updateEntryObject(LineType.FT,
						"FT   MOTIF           864..865\n" + 
						"FT                   /note=\"Di-leucine internalization motif\"\n" + 
						"FT                   /evidence=\"ECO:0000256|HAMAP-Rule:MF_04083\"\n"+
						"FT   REPEAT          206..213\n" + 
						"FT                   /note=\"CXXCXGXG motif\"\n" + 
						"FT                   /evidence=\"ECO:0000256|HAMAP-Rule:MF_01152\"\n"+
						"FT   REPEAT          228..235\n" + 
						"FT                   /note=\"CXXCXGXG motif\"\n" + 
						"FT                   /evidence=\"ECO:0000256|HAMAP-Rule:MF_01152\"");

		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
		entryProxy.updateEntryObject(LineType.FT,
						"FT   REGION          453..474\n" + 
						"FT                   /note=\"Putative leucine zipper motif\"\n" + 
						"FT                   /evidence=\"ECO:0000256|HAMAP-Rule:MF_04012\"\n"+
						"FT   ZN_FING         216..277\n" + 
						"FT                   /note=\"UBP-type\"\n" + 
						"FT                   /evidence=\"ECO:0000256|PROSITE-ProRule:PRU00502\"");

		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		searchEngine.printIndexContents();
	}

	@Test
	void domainFindEntryWithEvidenceLength() {
		String query = features(FeatureType.DOMAIN, "phosphatase");
		query =QueryBuilder.and(query, featureLength(FeatureType.DOMAIN, 10, 20));
		String evidence = "ECO_0000259";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.DOMAIN, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX4));
		assertThat(retrievedAccessions, not(hasItem(Q197B1)));
	}
	@Test
	void coiledFindEntryWithEvidenceLength() {
		String query = features(FeatureType.COILED, "*");
		query =QueryBuilder.and(query, featureLength(FeatureType.COILED, 10, 30));
		String evidence = "ECO_0000255";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.COILED, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q197B1));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	
	@Test
	void compbiasFindEntryWithEvidenceLength() {
		String query = features(FeatureType.COMPBIAS, "glu-rich");
		query =QueryBuilder.and(query, featureLength(FeatureType.COMPBIAS, 10, 30));
		String evidence = "ECO_0000256";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.COMPBIAS, evidence));

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	
	@Test
	void motifFindEntryWithEvidenceLength() {
		String query = features(FeatureType.MOTIF, "motif");
		query =QueryBuilder.and(query, featureLength(FeatureType.MOTIF, 2, 30));
		String evidence = "ECO_0000256";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.MOTIF, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	@Test
	void repeatFindEntryWithEvidenceLength() {
		String query = features(FeatureType.REPEAT, "motif");
		query =QueryBuilder.and(query, featureLength(FeatureType.REPEAT, 2, 30));
		String evidence = "ECO_0000256";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.REPEAT, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	@Test
	void regionFindEntryWithEvidenceLength() {
		String query = features(FeatureType.REGION, "motif");
		query =QueryBuilder.and(query, featureLength(FeatureType.REGION, 2, 30));
		String evidence = "ECO_0000256";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.REGION, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
	@Test
	void znfingFindEntryWithEvidenceLength() {
		String query = features(FeatureType.ZN_FING, "UBP");
		query =QueryBuilder.and(query, featureLength(FeatureType.ZN_FING, 2, 70));
		String evidence = "ECO_0000256";
		query =QueryBuilder.and(query, featureEvidence(FeatureType.ZN_FING, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0));
		assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
	}
}
