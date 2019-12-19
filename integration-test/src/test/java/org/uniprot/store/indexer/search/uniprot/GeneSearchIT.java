package org.uniprot.store.indexer.search.uniprot;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.UniProtField;

/** Tests if the Genes section has been indexed properly */
class GeneSearchIT {
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String COMMON_HLA_NAME = "HLA";

    // Entry 1
    private static final String ACCESSION1 = "Q197F4";
    private static final String NAME1 = "HLA-A";
    private static final String SYNONYM1_1 = "HLAA";
    // Entry 2
    private static final String ACCESSION2 = "Q197F5";
    private static final String NAME2 = "HLA-B";
    private static final String SYNONYM2_1 = "HLAB";
    private static final String COMMON_PPP2R_NAME = "PPP2R";
    // Entry 3
    private static final String ACCESSION3 = "Q197F6";
    private static final String NAME3 = "PPP2R5A";
    // Entry 4
    private static final String ACCESSION4 = "Q197F7";
    private static final String NAME4 = "PPP2R5D";
    // Entry 5
    private static final String ACCESSION5 = "Q197F8";
    private static final String NAME5 = "alkA";
    private static final String OLN5_1 = "b2068";
    // Entry 6
    private static final String ACCESSION6 = "Q197F9";
    private static final String NAME6 = "ATG16L1";
    private static final String ORF6_1_1 = "UNQ9393";
    private static final String ORF6_1_2 = "PRO34307";
    private static final String ORF6_1 = ORF6_1_1 + "/" + ORF6_1_2;
    // Entry 7
    private static final String ACCESSION7 = "Q197G0";
    private static final String NAME7 = "ABCC1";
    private static final String ORF7_1_PARTIAL = "T4K22";
    private static final String ORF7_1 = "T4K22.12";

    private static UniProtEntryObjectProxy entryProxy;
    private static int accessionId = 0;
    private List<String> tempSavedEntries = new ArrayList<>();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        GeneBuilder geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME1).addSynonym(SYNONYM1_1);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME2).addSynonym(SYNONYM2_1);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME3);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 4
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME4);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 5
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME5).addOLNName(OLN5_1);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION5));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 6
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME6).addORFName(ORF6_1);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION6));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 7
        geneBuilder = new GeneBuilder();
        geneBuilder.setName(NAME7).addORFName(ORF7_1);

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION7));
        entryProxy.updateEntryObject(LineType.GN, geneBuilder.buildGNLine());
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @AfterEach
    void after() {
        cleanTempEntries();
    }

    // exact gene queries ----------------------------------------------------
    @Test
    void incorrectExactGeneNameMatchesNoDocuments() {
        String query = exactGeneQuery("this is wrong");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void correctExactGeneNameMatchesEntry1() {
        String query = exactGeneQuery(NAME1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    // general gene queries ----------------------------------------------------
    @Test
    void nonExistentGeneNameMatchesNoDocuments() {
        String query = geneQuery("Unknown");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void fullGeneNameFromEntry1MatchesEntry1() {
        String query = geneQuery(NAME1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void fullGeneSynonymFromEntry1InLowerCaseMatchesEntry1() {
        String query = geneQuery(SYNONYM1_1.toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void fullGeneSynonymFromEntry2MatchesEntry2() {
        String query = geneQuery(SYNONYM2_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void partialCommonGeneNameDoesNotMatchAnyEntry() {
        String query = geneQuery("PP");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void partialCommonGeneNameWithWildCardMatchesEntry1And2() {
        String query = geneQuery(COMMON_HLA_NAME + "*");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION1, ACCESSION2));
    }

    @Test
    void orderedLocusNameFromEntry5MatchesEntry5() {
        String query = geneQuery(OLN5_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION5));
    }

    @Test
    void bothElementsORFFromEntry6MatchesEntry6() {
        String query = geneQuery(ORF6_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    void firstElementORFFromEntry6MatchesEntry6() {
        String query = geneQuery(ORF6_1_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    void secondElementORFFromEntry6MatchesEntry6() {
        String query = geneQuery(ORF6_1_2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION6));
    }

    @Test
    void fullORFFromEntry7MatchesEntry7() {
        String query = geneQuery(ORF7_1);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION7));
    }

    @Test
    void ORFNameUpToDotFromEntry7MatchesEntry7() {
        String query = geneQuery(ORF7_1_PARTIAL);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION7));
    }

    // Edge cases --------------------
    @Test
    void canFindGeneWithUnderScore() {
        String accession = newAccession();
        String geneName = "VARV_IND64_vel4_019";
        String query = geneQuery(geneName);

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void caseInsensitiveFindGeneWithUnderScores() {
        String accession = newAccession();
        String geneName = "VARV_IND64_vel4_019";
        String query = geneQuery(geneName.toLowerCase());

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void canUseFirstPartOfGeneNameToFindGeneWithUnderScores() {
        String accession = newAccession();
        String geneName = "VARV_IND64_vel4_019";
        String query = geneQuery("VARV");

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void canUseMiddlePartOfGeneNameToFindGeneWithUnderScores() {
        String accession = newAccession();
        String geneName = "VARV_IND64_vel4_019";
        String query = geneQuery("vel4");

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void canUseMiddlePartsOfGeneNameToFindGeneWithUnderScores() {
        String accession = newAccession();
        String geneName = "VARV_IND64_vel4_019";
        String query = geneQuery("IND64_vel4");

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void canFindGeneThatIsOnlyANumber() {
        String accession = newAccession();
        String geneName = "62";
        String query = geneQuery(geneName);

        index(accession, new GeneBuilder().setName(geneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    @Test
    void canFindGenesContainingSpecialChars() {
        List<String> valuesThatRequireEscaping =
                asList(
                        "+",
                        "-",
                        "&",
                        "|",
                        "!",
                        "(",
                        ")",
                        "{EVIDENCE}",
                        "[",
                        "]",
                        "^",
                        "\"",
                        "~",
                        "?",
                        ":",
                        "/");

        for (String toEscape : valuesThatRequireEscaping) {
            String accession = newAccession();
            String geneName = "hello" + toEscape + "world";
            System.out.println(geneName);
            String query = geneQuery(geneName);

            index(accession, new GeneBuilder().setName(geneName).buildGNLine());
            QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, contains(accession));

            searchEngine.removeEntry(accession);
        }
    }

    @Disabled
    @Test
    void canFindGeneViaAlternativeSpellingFromSynonymList() {
        // ensure synonyms in are used:
        //
        // uniprot-data-services/data-service-deployments/src/main/distros/solr-conf/homes/uniprot-cores/uniprot/conf/synonyms.txt
        String accession = newAccession();
        String indexGeneName = "hemoglobin tumor";
        String queryGeneName = "haemoglobin tumour";
        String query = geneQuery(queryGeneName);

        index(accession, new GeneBuilder().setName(indexGeneName).buildGNLine());
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(accession));
    }

    private String newAccession() {
        return "P" + String.format("%05d", accessionId++);
    }

    private void index(String accession, String gnLine) {
        tempSavedEntries.add(accession);
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, accession));
        entryProxy.updateEntryObject(LineType.GN, gnLine);
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
    }

    private void cleanTempEntries() {
        tempSavedEntries.forEach(e -> searchEngine.removeEntry(e));
    }

    private String geneQuery(String value) {
        return query(UniProtField.Search.gene, value);
    }

    private String exactGeneQuery(String value) {
        return query(UniProtField.Search.gene_exact, value);
    }

    /**
     * Builder that collects the necessary parameters to build the GN line, and then returns a flat
     * file representation of the GN line with the given parameters.
     */
    private static class GeneBuilder {
        private String name;
        private List<String> synonyms;
        private List<String> OLNNames;
        private List<String> ORFNames;

        GeneBuilder() {
            synonyms = new ArrayList<>();
            OLNNames = new ArrayList<>();
            ORFNames = new ArrayList<>();
        }

        public GeneBuilder setName(String name) {
            this.name = name;
            return this;
        }

        GeneBuilder addSynonym(String synonym) {
            synonyms.add(synonym);
            return this;
        }

        GeneBuilder addOLNName(String OLNName) {
            OLNNames.add(OLNName);
            return this;
        }

        GeneBuilder addORFName(String ORFName) {
            ORFNames.add(ORFName);
            return this;
        }

        String buildGNLine() {
            StringBuilder gnLine = new StringBuilder("GN   ");
            gnLine.append(buildName());

            String synonymLine = buildMultiNameSection("Synonyms", synonyms);
            if (!synonymLine.isEmpty()) {
                gnLine.append(" ").append(synonymLine);
            }

            String OLNLine = buildMultiNameSection("OrderedLocusNames", OLNNames);
            if (!OLNLine.isEmpty()) {
                gnLine.append(" ").append(OLNLine);
            }

            String ORFLine = buildMultiNameSection("ORFNames", ORFNames);
            if (!ORFLine.isEmpty()) {
                gnLine.append(" ").append(ORFLine);
            }

            return gnLine.toString();
        }

        private String buildName() {
            String name = "";

            if (this.name != null && !this.name.trim().isEmpty()) {
                name = "Name=" + this.name + ";";
            }

            return name;
        }

        /**
         * Convenience method that converts a list of names into a section in the GN line
         *
         * @param nameType the name section that will be built, i.e. synonyms, OLNNames, ORFNames
         * @param names the names that make up the section
         * @return a flat file version of the section being built
         */
        private String buildMultiNameSection(String nameType, List<String> names) {
            String multiNameString = "";

            if (!names.isEmpty()) {
                multiNameString =
                        createMultiElementFFLine(
                                nameType + "=", ",", ";", names.toArray(new String[0]));
            }

            return multiNameString;
        }
    }
}
