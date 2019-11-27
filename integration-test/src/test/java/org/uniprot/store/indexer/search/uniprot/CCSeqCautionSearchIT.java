package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.domain2.UniProtKBSearchFields;
import org.uniprot.store.search.field.QueryBuilder;

class CCSeqCautionSearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZY3 = "Q6GZY3";
    private static final String Q197B6 = "Q197B6";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q196W5 = "Q196W5";
    private static final String Q6GZN7 = "Q6GZN7";
    private static final String Q6V4H0 = "Q6V4H0";

    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- FUNCTION: Transcription activation. {ECO:0000305}.\n"
                        + "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=CAA36850.1; Type=Frameshift;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=BAC87262.1; Type=Erroneous initiation; Evidence={ECO:0000305};\n"
                        + "CC       Sequence=CAC85331.1; Type=Frameshift; Evidence={ECO:0000305};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAG52403.1; Type=Erroneous gene model prediction; Evidence={ECO:0000305};\n"
                        + "CC       Sequence=ABW87767.1; Type=Erroneous initiation; Note=Translation N-terminally extended.; Evidence={ECO:0000305};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=BAB15298.1; Type=Erroneous termination; Note=Translated as Glu.; Evidence={ECO:0000269};\n"
                        + "CC       Sequence=BAC85554.1; Type=Frameshift; Evidence={ECO:0000269};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q196W5));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAL57305.1; Type=Erroneous gene model prediction; Evidence={ECO:0000305};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAB61673.1; Type=Erroneous translation; Note=Wrong choice of CDS.; Evidence={ECO:0000305};\n"
                        + "CC       Sequence=AAI15038.1; Type=Erroneous initiation; Note=Translation N-terminally extended.; Evidence={ECO:0000305};\n"
                        + "CC       Sequence=AAI15038.1; Type=Erroneous termination; Note=Translated as Gln.; Evidence={ECO:0000305};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- SEQUENCE CAUTION:\n"
                        + "CC       Sequence=AAH39610.1; Type=Miscellaneous discrepancy; Note=Contaminating sequence. Potential poly-A sequence.; Evidence={ECO:0000305};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void findAllSeqCaution() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc"), "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(
                retrievedAccessions,
                hasItems(Q6GZX4, Q6GZX3, Q6GZY3, Q197B6, Q196W5, Q6GZN7, Q6V4H0));
    }

    @Test
    void findSeqCaution() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc"), "Translated");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7, Q197B6));
    }

    @Test
    void findSeqCautionWithEvidence() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc"), "Translated");
        String evidence = "ECO_0000305";
        query =
                QueryBuilder.and(
                        query, query(UniProtKBSearchFields.INSTANCE.getField("ccev_sc"), evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q197B6)));
    }

    @Test
    void findSeqCautionWithManualEvidence() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc"), "Translated");
        String evidence = "manual";
        query =
                QueryBuilder.and(
                        query, query(UniProtKBSearchFields.INSTANCE.getField("ccev_sc"), evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6));
    }

    @Test
    void findSeqCautionWithAutomaticEvidence() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc"), "Translated");
        String evidence = "automatic";
        query =
                QueryBuilder.and(
                        query, query(UniProtKBSearchFields.INSTANCE.getField("ccev_sc"), evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void findAllSeqCautionFrameshift() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_framesh"), "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3, Q197B6));
    }

    @Test
    void findAllSeqCautionErrorInit() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_einit"), "extended");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZY3, Q6GZN7));
    }

    @Test
    void findAllSeqCautionErrorPredict() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_epred"), "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZY3, Q196W5));
    }

    @Test
    void findAllSeqCautionErrorTerm() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_eterm"), "Translated");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q6GZN7));
    }

    @Test
    void findAllSeqCautionErrorTranslation() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_etran"), "choice");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
    }

    @Test
    void findAllSeqCautionMisc() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_misc"), "sequence");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
    }

    @Test
    void findAllSeqCautionMiscWithEv() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_misc"), "sequence");
        String evidence = "ECO_0000305";
        query =
                QueryBuilder.and(
                        query,
                        query(UniProtKBSearchFields.INSTANCE.getField("ccev_sc_misc"), evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
    }

    @Test
    void findAllSeqCautionMiscWithEvEmptu() {
        String query = query(UniProtKBSearchFields.INSTANCE.getField("cc_sc_misc"), "sequence");
        String evidence = "ECO_0000268";
        query =
                QueryBuilder.and(
                        query,
                        query(UniProtKBSearchFields.INSTANCE.getField("ccev_sc_misc"), evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, empty());
    }
}
