package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.search.field.QueryBuilder;

class CCBpcpSearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZY3 = "Q6GZY3";
    private static final String Q197B6 = "Q197B6";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q12345 = "Q12345";

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
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Absorption:\n"
                        + "CC         Abs(max)=411 nm {ECO:0000269|PubMed:11526234};\n"
                        + "CC         Note=The absorbance spectrum of deoxy-GLB3 is unique as the\n"
                        + "CC         protein forms a transient six-coordinate structure with\n"
                        + "CC         absorption peaks at 538 and 565 nm after reduction and\n"
                        + "CC         deoxygenation, which slowly converts to a five-coordinate\n"
                        + "CC         structure with an absorption peak at 548 nm.;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         KM=4.1 mM for carboxyspermidine {ECO:0000250};\n"
                        + "CC         KM=2.1 mM for carboxynorspermidine\n"
                        + "CC         {ECO:0000250};\n"
                        + "CC         Note=KM values are given with the protein sequence containing\n"
                        + "CC         Glu instead of Lys at position 184, the effect of the variation\n"
                        + "CC         on activity is unclear.;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZY3));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         Vmax=685 umol/min/mg enzyme for the nitrite reductase activity\n"
                        + "CC         {ECO:0000269|PubMed:11004582};\n"
                        + "CC         Vmax=1.0 umol/min/mg enzyme for the sulfite reductase activity\n"
                        + "CC         {ECO:0000269|PubMed:11004582};\n"
                        + "CC       Redox potential:\n"
                        + "CC         E(0) is -50 mV for the lysine-coordinated heme 1.\n"
                        + "CC         {ECO:0000269|PubMed:22519292};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B6));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         KM=1.03 uM for riboflavin (at pH 7.5)\n"
                        + "CC         {ECO:0000269|PubMed:16183635};\n"
                        + "CC         KM=2 uM for ATP (at pH 7.5) {ECO:0000269|PubMed:16183635};\n"
                        + "CC         KM=6.74 uM for FMN (at pH 7.5) {ECO:0000269|PubMed:16183635};\n"
                        + "CC       pH dependence:\n"
                        + "CC         Optimum pH is acidic (5.5-6.0) for FMN phosphatase activity and\n"
                        + "CC         basic for riboflavin kinase activity.\n"
                        + "CC         {ECO:0000269|PubMed:16183635, ECO:0000305|PubMed:22002057};\n"
                        + "CC       Temperature dependence:\n"
                        + "CC         Optimum temperature is 55 degrees Celsius.\n"
                        + "CC         {ECO:0000305|PubMed:22002057};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.CC,
                "CC   -!- INTERACTION:\n"
                        + "CC       Q6GZX4; Q41009: TOC34; Xeno; NbExp=2; IntAct=EBI-1803304, EBI-638506;\n"
                        + "CC   -!- BIOPHYSICOCHEMICAL PROPERTIES:\n"
                        + "CC       Kinetic parameters:\n"
                        + "CC         KM=620 uM for O-phospho-L-serine (at 70 degrees Celsius and at\n"
                        + "CC         pH 7.5) {ECO:0000269|PubMed:12051918};\n"
                        + "CC       pH dependence:\n"
                        + "CC         Optimum pH is 7.5. {ECO:0000250};\n"
                        + "CC       Temperature dependence:\n"
                        + "CC         Optimum temperature is 70 degrees Celsius.\n"
                        + "CC         {ECO:0000269|PubMed:12051918};");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void findBPC() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("cc_bpcp"),
                        "protein");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX4, Q6GZX3));
    }

    @Test
    void findBPCPWithAbsorption() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_absorption"),
                        "spectrum");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX4));
    }

    @Test
    void findBPCPWithKinetics() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_kinetics"),
                        "*");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX3, Q6GZY3, Q197B6, Q12345));
    }

    @Test
    void findBPCPWithKinetics2() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_kinetics"),
                        "carboxyspermidine");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX3));
    }

    @Test
    void findBPCPWithPhDependence() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_ph_dependence"),
                        "optimum");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B6, Q12345));
    }

    @Test
    void findBPCPWithTempDependence() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_temp_dependence"),
                        "temperature");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q12345, Q197B6));
    }

    @Test
    void findBPCPWithRedox() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("cc_bpcp_redox_potential"),
                        "heme");
        QueryResponse response = searchEngine.getQueryResponse(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZY3));
    }

    private String query(SearchFieldItem field, String fieldValue) {
        return QueryBuilder.query(field.getFieldName(), fieldValue);
    }
}
