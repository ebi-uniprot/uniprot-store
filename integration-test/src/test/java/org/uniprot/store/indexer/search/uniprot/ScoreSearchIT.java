package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

class ScoreSearchIT {
			private static final String Q6GZX4 = "Q6GZX4";
			private static final String Q6GZX3 = "Q6GZX3";
			private static final String Q6GZY3 = "Q6GZY3";
			private static final String Q197B6 = "Q197B6";
	    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
	    private static final String Q196W5 = "Q196W5";
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
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
	        entryProxy.updateEntryObject(LineType.CC, "CC   -!- FUNCTION: Transcription activation. {ECO:0000305}.\n"+
	        		"CC   -!- SEQUENCE CAUTION:\n"
					+ "CC       Sequence=CAA36850.1; Type=Frameshift; Positions=496;");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- SUBCELLULAR LOCATION: This-is-a-word Host membrane extraWord {ECO:0000305}; Single-pass\n" +
	                        "CC       membrane protein {ECO:0000305}.\n"
	                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" + 
	                        "CC       Absorption:\n" + 
	                        "CC         Abs(max)=~715 nm;\n" + 
	                        "CC         Note=Emission maxima at 735 nm. {ECO:0000269|PubMed:11553743};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- SUBCELLULAR LOCATION: This-is-a Host membrane; Single-pass\n" +
	                        "CC       membrane protein. Note=Localizes at mid-cell.");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- SIMILARITY: Belongs to the protein kinase superfamily. Ser/Thr\n" +
	                        "CC       protein kinase family. {ECO:0000255|PROSITE-ProRule:PRU00159}.\n" +
	                        "CC   -!- SIMILARITY: Contains 1 protein kinase domain.\n" +
	                        "CC       {ECO:0000255|PROSITE-ProRule:PRU00159}.");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q196W5));
	        entryProxy.updateEntryObject(LineType.CC, "CC   -!- COFACTOR:\n" +
	                "CC       Name=Zn(2+); Xref=ChEBI:CHEBI:29105; Evidence={ECO:0000250};\n" +
	                "CC       Note=Binds 1 zinc ion per subunit. {ECO:0000250};\n" +
	                "CC   -!- SUBCELLULAR LOCATION: Secreted {ECO:0000305}.\n" +
	                "CC   -!- DOMAIN: The conserved cysteine present in the cysteine-switch\n" +
	                "CC       motif binds the catalytic zinc ion, thus inhibiting the enzyme.\n" +
	                "CC       The dissociation of the cysteine from the zinc ion upon the\n" +
	                "CC       activation-peptide release activates the enzyme.\n" +
	                "CC   -!- SIMILARITY: Belongs to the peptidase M10A family. {ECO:0000305}.");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
	        entryProxy
	                .updateEntryObject(LineType.CC, "CC   -!- CATALYTIC ACTIVITY:\n" + 
	                        "CC       Reaction=O2 + 2 R'C(R)SH = H2O2 + R'C(R)S-S(R)CR';\n" + 
	                        "CC         Xref=Rhea:RHEA:17357, ChEBI:CHEBI:15379, ChEBI:CHEBI:16240,\n" + 
	                        "CC         ChEBI:CHEBI:16520, ChEBI:CHEBI:17412; EC=1.8.3.2;");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
	        entryProxy.updateEntryObject(LineType.CC,
	                "CC   -!- CATALYTIC ACTIVITY:\n" + 
	                "CC       Reaction=(6E)-8-hydroxygeraniol + 2 NADP(+) = (6E)-8-oxogeranial +\n" + 
	                "CC         2 H(+) + 2 NADPH; Xref=Rhea:RHEA:32659, ChEBI:CHEBI:15378,\n" + 
	                "CC         ChEBI:CHEBI:57783, ChEBI:CHEBI:58349, ChEBI:CHEBI:64235,\n" + 
	                "CC         ChEBI:CHEBI:64239; EC=1.1.1.324; Evidence={ECO:0000269|Ref.1};\n"
	                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" + 
	                        "CC       Kinetic parameters:\n" + 
	                        "CC         KM=6.9 uM for Ins(1,3,4,5)P(4) {ECO:0000269|PubMed:9359836};\n" + 
	                        "CC         Vmax=302 pmol/min/ug enzyme {ECO:0000269|PubMed:9359836};\n"
	                      + 	"CC       Redox potential:\n" +
	        				    "CC         E(0) is about 178 mV. {ECO:0000269|PubMed:10433554};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
	        entryProxy.updateEntryObject(LineType.CC, "CC   -!- ALTERNATIVE PRODUCTS:\n" +
	                "CC       Event=Alternative splicing; Named isoforms=2;\n" +
	                "CC       Name=1;\n" +
	                "CC         IsoId=P48347-1; Sequence=Displayed;\n" +
	                "CC       Name=2;\n" +
	                "CC         IsoId=P48347-2; Sequence=VSP_008972;");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        // --------------
	        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
	        entryProxy.updateEntryObject(LineType.CC, "CC   -!- INTERACTION:\n" +
	                "CC       Q41009:TOC34 (xeno); NbExp=2; IntAct=EBI-1803304, EBI-638506;\n"
	                + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n" + 
	                "CC       Kinetic parameters:\n" + 
	                "CC         KM=620 uM for O-phospho-L-serine (at 70 degrees Celsius and at\n" + 
	                "CC         pH 7.5) {ECO:0000269|PubMed:12051918};\n" + 
	                "CC       pH dependence:\n" + 
	                "CC         Optimum pH is 7.5. {ECO:0000269|PubMed:12051918};\n" + 
	                "CC       Temperature dependence:\n" + 
	                "CC         Optimum temperature is 70 degrees Celsius.\n" + 
	                "CC         {ECO:0000269|PubMed:12051918};");
	        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

	        searchEngine.printIndexContents();
	    }
	    @Test
	    void score1() {
	    	String query= query(UniProtField.Search.annotation_score, "1");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, empty());
	    }
	    @Test
	    void score2() {
	    	String query= query(UniProtField.Search.annotation_score, "2");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3, Q6GZY3, Q197B6, Q6GZN7));
	    }
	    @Test
	    void score3() {
	    	String query= query(UniProtField.Search.annotation_score, "3");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, hasItems(Q6V4H0, Q12345));
	    }
	    @Test
	    void score4() {
	    	String query= query(UniProtField.Search.annotation_score, "4");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, empty());
	    }
	    @Test
	    void score5() {
	    	String query= query(UniProtField.Search.annotation_score, "5");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, empty());
	    }
	    @Test
	    void score6() {
	    	String query= query(UniProtField.Search.annotation_score, "6");
    		QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            System.out.println(retrievedAccessions);
            assertThat(retrievedAccessions, empty());
	    }
}
