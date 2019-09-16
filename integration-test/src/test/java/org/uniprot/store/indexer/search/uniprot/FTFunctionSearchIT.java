package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
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
import org.uniprot.store.search.field.UniProtField;


public class FTFunctionSearchIT {
	public static final String Q6GZX4 = "Q6GZX4";
	public static final String Q197B1 = "Q197B1";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	private static final String Q12345 = "Q12345";
	private static final String P48347 = "P48347";
	@RegisterExtension
	public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	@BeforeAll
	public static void populateIndexWithTestData() throws IOException {
		// a test entry object that can be modified and added to index
		InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
		UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   SITE         11     11       Substrate for Tat translocation through\n"
						+ "FT                                the endosomal membrane.\n"
						+ "FT                                {ECO:0000256|HAMAP-Rule:MF_04079}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
		entryProxy.updateEntryObject(LineType.FT, "FT   METAL       151    151       Magnesium. {ECO:0000255|HAMAP-\n"
				+ "FT                                Rule:MF_01227}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   BINDING    1516   1516       Substrate. {ECO:0000250}.\n"
						+ "FT   ACT_SITE   1380   1380       Phosphocysteine intermediate.\n"
						+ "FT                                {ECO:0000250}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
		entryProxy.updateEntryObject(LineType.FT,
				"FT   CA_BIND     228    229       Second part of site. {ECO:0000250}.\n"
				+ "FT   DNA_BIND     13     16       {ECO:0000250}.\n"
				+ "FT   NP_BIND      81     85       NADP. {ECO:0000250|UniProtKB:Q4V8K1}.");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
       
		searchEngine.printIndexContents();
	}

	@Test
	public void sitesFindTwoEntry() {
		String query = query(UniProtField.Search.ft_sites, "Substrate");
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345, Q6GZX4));

	}

	@Test
	public void sitesFindTwoEntryWithLength() {
		String query = query(UniProtField.Search.ft_sites, "Substrate");
		query = QueryBuilder.and(query, QueryBuilder.rangeQuery(UniProtField.Search.ftlen_sites.name(), 1, 3));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345, Q6GZX4));
	}

	@Test
	public void sitesFindEntryWithLengthAndEvidence() {
		String query = query(UniProtField.Search.ft_sites, "Substrate");
		query = QueryBuilder.and(query, QueryBuilder.rangeQuery(UniProtField.Search.ftlen_sites.name(), 1, 3));
		String evidence = "ECO_0000256";
		query = QueryBuilder.and(query, query(UniProtField.Search.ftev_sites, evidence));

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZX4));
		assertThat(retrievedAccessions, not(hasItem(Q12345)));
	}

	@Test
	public void siteFindEntry() {
		String query = features(FeatureType.SITE, "translocation.");

		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(Q6GZX4));
	}

	@Test
	public void activeSiteFindEntry() {
		String query = features(FeatureType.ACT_SITE, "intermediate.");
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.ACT_SITE, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(Q12345));
	}

	@Test
	public void metalSiteFindEntry() {
		String query = features(FeatureType.METAL, "Magnesium.");
		query = QueryBuilder.and(query, featureLength(FeatureType.METAL, 1, 2));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(Q197B1));
	}

	@Test
	public void metalSiteFindNonEntry() {
		String query = features(FeatureType.METAL, "Magnesium.");
		query = QueryBuilder.and(query, featureLength(FeatureType.METAL, 3, 5));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, empty());
	}

	@Test
	public void bindingSiteFindEntry() {
		String query = features(FeatureType.BINDING, "Substrate.");
		query = QueryBuilder.and(query, featureLength(FeatureType.BINDING, 1, 2));
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.BINDING, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(Q12345));
	}
	@Test
	public void caBindFindEntry() {
		String query = features(FeatureType.CA_BIND, "site");
		query = QueryBuilder.and(query, featureLength(FeatureType.CA_BIND, 1, 2));
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.CA_BIND, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(P48347));
	}
	@Test
	public void dnaBindFindEntry() {
		String query = features(FeatureType.DNA_BIND, "*");
		query = QueryBuilder.and(query, featureLength(FeatureType.DNA_BIND, 1, 6));
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.DNA_BIND, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(P48347));
	}
	@Test
	public void npBindFindEntry() {
		String query = features(FeatureType.NP_BIND, "NADP");
		query = QueryBuilder.and(query, featureLength(FeatureType.NP_BIND, 1, 6));
		String evidence = "ECO_0000250";
		query = QueryBuilder.and(query, featureEvidence(FeatureType.NP_BIND, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);

		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		assertThat(retrievedAccessions, contains(P48347));
	}
}
