package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

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


class CCAPSearchIT {
	private static final String Q6GZX4 = "Q6GZX4";
	private static final String Q6GZX3 = "Q6GZX3";
	private static final String Q6GZY3 = "Q6GZY3";
	private static final String Q197B6 = "Q197B6";
	private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	private static final String Q6GZN7 = "Q6GZN7";
	private static final String Q6V4H0 = "Q6V4H0";
	private static final String P48347 = "P48347";
	private static final String Q12345 = "Q12345";

	@RegisterExtension
	static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

	@BeforeAll
	static void populateIndexWithTestData() throws IOException {
		// a test entry object that can be modified and added to index
		InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
		UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- ALTERNATIVE PRODUCTS:\n" + 
				"CC       Event=Alternative splicing, Alternative initiation; Named isoforms=10;\n" + 
				"CC         Comment=Isoform C-alpha and isoform C-beta are the strongest\n" + 
				"CC         activator of gene, transcription followed by isoform A-alpha and\n" + 
				"CC         isoform A-beta, whereas isoform B-alpha and isoform B-beta are\n" + 
				"CC         the weakest. Isoform B-alpha, isoform B-beta, isoform C-alpha\n" + 
				"CC         and isoform C-beta, both present in T-cells, can modulate their\n" + 
				"CC         transcriptional activity. {ECO:0000303|PubMed:10835424};\n" + 
				"CC       Name=C-alpha;\n" + 
				"CC         IsoId=O95644-1; Sequence=Displayed;\n" + 
				"CC       Name=A-alpha; Synonyms=IA-VIII;\n" + 
				"CC         IsoId=O95644-2; Sequence=VSP_005591, VSP_005592;\n" + 
				"CC       Name=A-beta; Synonyms=IB-VIII;\n" + 
				"CC         IsoId=O95644-3; Sequence=VSP_005590, VSP_005591, VSP_005592;\n" + 
				"CC       Name=B-alpha; Synonyms=IA-IXS;\n" + 
				"CC         IsoId=O95644-4; Sequence=VSP_005593;\n" + 
				"CC       Name=B-beta; Synonyms=IB-IXS;\n" + 
				"CC         IsoId=O95644-5; Sequence=VSP_005590, VSP_005593;\n" + 
				"CC       Name=C-beta; Synonyms=IB-IXL;\n" + 
				"CC         IsoId=O95644-6; Sequence=VSP_005590;\n" + 
				"CC       Name=A-alpha';\n" + 
				"CC         IsoId=O95644-8; Sequence=VSP_018978, VSP_005591, VSP_005592;\n" + 
				"CC         Note=Produced by alternative initiation at Met-37 of isoform\n" + 
				"CC         A-alpha. No experimental confirmation available.;\n" + 
				"CC       Name=IA-deltaIX;\n" + 
				"CC         IsoId=O95644-10; Sequence=VSP_047820;\n" + 
				"CC       Name=IB-deltaIX;\n" + 
				"CC         IsoId=O95644-11; Sequence=VSP_005590, VSP_047820;\n" + 
				"CC       Name=10;\n" + 
				"CC         IsoId=O95644-17; Sequence=VSP_053806, VSP_005593;\n" + 
				"CC         Note=No experimental evidence available.;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- ALTERNATIVE PRODUCTS:\n" + 
				"CC       Event=Alternative promoter usage, Alternative splicing; Named isoforms=2;\n" + 
				"CC         Comment=A number of isoforms are produced by alternative\n" + 
				"CC         promoter usage transcription. The use of alternative promoters apparently\n" + 
				"CC         enables the type IV hexokinase gene to be regulated by insulin\n" + 
				"CC         in the liver and glucose in the beta cell. This may constitute\n" + 
				"CC         an important feedback loop for maintaining glucose homeostasis. {ECO:0000269|PubMed:10835425};\n" + 
				"CC       Name=1;\n" + 
				"CC         IsoId=P52792-1; Sequence=Displayed;\n" + 
				"CC       Name=2;\n" + 
				"CC         IsoId=P52792-2; Sequence=VSP_002076;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- ALTERNATIVE PRODUCTS:\n" + "CC       Event=Alternative splicing; Named isoforms=2;\n"
						+ "CC       Name=1;\n" + "CC         IsoId=P48347-1; Sequence=Displayed;\n"
						+ "CC       Name=2;\n" + "CC         IsoId=P48347-2; Sequence=VSP_008972;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		// --------------
		entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
		entryProxy.updateEntryObject(LineType.CC,
				"CC   -!- ALTERNATIVE PRODUCTS:\n" + 
				"CC       Event=Ribosomal frameshifting; Named isoforms=2;\n" + 
				"CC       Name=Replicase polyprotein 1ab; Synonyms=pp1ab;\n" + 
				"CC         IsoId=Q98VG9-1; Sequence=Displayed;\n" + 
				"CC         Note=Produced by -1 ribosomal frameshifting at the 1a-1b genes\n" + 
				"CC         boundary.;\n" + 
				"CC       Name=Replicase polyprotein 1a; Synonyms=pp1a, ORF1a polyprotein;\n" + 
				"CC         IsoId=Q98VG9-2; Sequence=VSP_032885;\n" + 
				"CC         Note=Produced by conventional translation.;");
		searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

		searchEngine.printIndexContents();
	}

	@Test
	void testAP() {
		String query = query(UniProtField.Search.cc_ap, "splicing");
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(P48347, Q6GZN7,Q6V4H0 ));
	
	}
	@Test
	void testAPEv() {
		String evidence ="ECO_0000269";
		String query = query(UniProtField.Search.cc_ap, "splicing");
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0 ));
		assertThat(retrievedAccessions, not(hasItem(Q6GZN7)));
	
	}
	
	@Test
	void testAPOnlyEv() {
		String evidence ="ECO_0000269";
		String query = query(UniProtField.Search.ccev_ap, evidence);
	//	query = query.and(UniProtQueryBuilder.query(UniProtField.Search.ccev_ap, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0 ));
		assertThat(retrievedAccessions, not(hasItem(Q6GZN7)));
	
	}
	@Test
	void testAPManualEv() {
		String evidence ="manual";
		String query = query(UniProtField.Search.cc_ap, "splicing");
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7, Q6V4H0 ));
		assertThat(retrievedAccessions, not(hasItem(P48347)));
	
	}
	
	@Test
	void testApApu() {
		String query = query(UniProtField.Search.cc_ap_apu, "insulin");
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0 ));
		assertThat(retrievedAccessions, not(hasItem(Q6GZN7)));
	}
	
	@Test
	void testApApuEvidenceOne() {
		String query = query(UniProtField.Search.cc_ap_apu, "insulin");
		String evidence ="ECO_0000269";
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap_apu, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0 ));
		assertThat(retrievedAccessions, not(hasItem(Q6GZN7)));
	}
	@Test
	void testApApuEvidenceNone() {
		String query = query(UniProtField.Search.cc_ap_apu, "insulin");
		String evidence ="ECO_0000303";
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap_apu, evidence));
		System.out.println(query);
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, empty());
	}
	
	@Test
	void testApAs() {
		String query = query(UniProtField.Search.cc_ap_as, "transcription");
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6V4H0 , Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(P48347)));
	}
	@Test
	void testApAsEvidenceOne() {
		String query = query(UniProtField.Search.cc_ap_as, "transcription");
		String evidence ="ECO_0000303";
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap_as, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems( Q6GZN7));
		assertThat(retrievedAccessions, not(hasItem(Q6V4H0)));
	}
	@Test
	void testApAi() {
		String query = query(UniProtField.Search.cc_ap_ai, "transcription");
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7 ));
		assertThat(retrievedAccessions, not(hasItem(Q6V4H0)));
	}
	@Test
	void testApAiEvidence() {
		String query = query(UniProtField.Search.cc_ap_ai, "transcription");
		String evidence ="ECO_0000303";
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap_ai, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q6GZN7 ));
		assertThat(retrievedAccessions, not(hasItem(Q6V4H0)));
	}
	@Test
	void testApAiEvidenceNone() {
		String query = query(UniProtField.Search.cc_ap_ai, "transcription");
		String evidence ="ECO_0000269";
		query = QueryBuilder.and(query, query(UniProtField.Search.ccev_ap_ai, evidence));
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, empty());
	}
	@Test
	void testApRf() {
		String query = query(UniProtField.Search.cc_ap_rf, "*");
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, hasItems(Q12345 ));
		assertThat(retrievedAccessions, not(hasItem(Q6GZN7)));
	}
	@Test
	void testApRfEvidence() {
		String query = query(UniProtField.Search.cc_ap_rf, "*");
		String evidence ="ECO_0000269";
		String query2 = query(UniProtField.Search.ccev_ap_rf, evidence);
		query = QueryBuilder.and(query, query2);
		QueryResponse response = searchEngine.getQueryResponse(query);
		List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
		System.out.println(retrievedAccessions);
		assertThat(retrievedAccessions, empty());
	}
	private String query(UniProtField.Search field, String fieldValue) {
		return QueryBuilder.query(field.name(), fieldValue);
	}
}
