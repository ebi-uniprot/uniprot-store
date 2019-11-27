package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.query;

/** Tests showing the behaviour of searching DR fields */
class DRSearchIT {
    private static final String GO_1 = "T1TTT2";
    private static final String GO_2 = "T1TTT3";
    private static final String GO_3 = "T1TTU9";
    private static final String REF_SEQ_1 = "T1TTT4";
    private static final String REF_SEQ_2 = "T1TTT5";
    private static final String EMBL_1 = "T1TTT6";
    private static final String EMBL_2 = "T1TTT7";
    private static final String EMBL_3 = "T1TTT8";
    private static final String INTERPRO_1 = "T1TTT9";
    private static final String INTERPRO_2 = "T1TTT0";
    private static final String ALLTERGOME_1 = "T1TTU1";
    private static final String ARACHNOSERVER_1 = "T1TTU2";
    private static final String ARACHNOSERVER_2 = "T1TTU0";
    private static final String CCDS_1 = "T1TTU3";
    private static final String ZFIN_1 = "T1TTU4";
    private static final String TCDB_1 = "T1TTU5";
    private static final String TCDB_2 = "T1TTU6";
    private static final String TCDB_3 = "T1TTU7";
    private static final String GENE3D_1 = "T1TTU8";

    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // GO refs
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_1));
        entryProxy.updateEntryObject(
                LineType.DR, "DR   GO; GO:0033644; C:host cell membrane; IEA:UniProtKB-KW.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_2));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0033644; C:host wheresTheSundayTimes membrane; IEA:UniProtKB-KW.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GO_3));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   GO; GO:0000175; F:3'-5'-exoribonuclease activity; IBA:GO_Central.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // RefSeq refs
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, REF_SEQ_1));
        entryProxy.updateEntryObject(LineType.DR, "DR   RefSeq; YP_654585.1; NC_008187.1.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, REF_SEQ_2));
        entryProxy.updateEntryObject(LineType.DR, "DR   RefSeq; YP_654585.2; NC_008187.2.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // EMBL refs
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, EMBL_1));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   EMBL; AY548484; AAT09661.1; -; Genomic_DNA.\n"
                        + "DR   EMBL; AY548489; AAT09662.1; -; Genomic_DNA.");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, EMBL_2));
        entryProxy.updateEntryObject(
                LineType.DR, "DR   EMBL; AY548484; AAT09661.2; -; Genomic_DNA.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, EMBL_3));
        entryProxy.updateEntryObject(
                LineType.DR, "DR   EMBL; BY548484; BAT09661.1; -; Genomic_DNA.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // DR   InterPro
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, INTERPRO_1));
        entryProxy.updateEntryObject(LineType.DR, "DR   InterPro; IPR004251; Vaccinia_virus_A16.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, INTERPRO_2));
        entryProxy.updateEntryObject(LineType.DR, "DR   InterPro; IPR004251; Vaccinia_virus_A17.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Misc DRs
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ALLTERGOME_1));
        entryProxy.updateEntryObject(LineType.DR, "DR   Allergome; 10031; Api m 12.0101.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ARACHNOSERVER_1));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   ArachnoServer; AS001207; Sphingomyelinase D-like LaSicTox alphaclone2 (fragment).");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ARACHNOSERVER_2));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   ArachnoServer; AS001207; Sphingomyelinase D-like LaSicTox alphaclone (fragment).");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, CCDS_1));
        entryProxy.updateEntryObject(LineType.DR, "DR   CCDS; CCDS10001.1; -. [Q92994-1]");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ZFIN_1));
        entryProxy.updateEntryObject(
                LineType.DR, "DR   ZFIN; ZDB-GENE-091204-381; si:ch211-120j21.1.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, TCDB_1));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   TCDB; 9.C.15.1.1; the animal calmodulin-dependent e.r. secretion pathway (csp) family.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, TCDB_2));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   TCDB; 9.B.67.5.1; the putative inorganic carbon (hco3(-)) transporter/o-brownianntigen polymerase (ict/oap) family zebra-feather-brownian-monster.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, TCDB_3));
        entryProxy.updateEntryObject(
                LineType.DR,
                "DR   TCDB; 3.D.6.1.1; the putative ion (h(+) or na(+))-translocating nadh:ferredoxin oxidoreductase (nfo) family zebra-feather-another-monster.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, GENE3D_1));
        entryProxy.updateEntryObject(LineType.DR, "DR   Gene3D; 1.10.533.10; -; 1.");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void gene3dHitId() {
        String query = xref("GENE3D", "1.10.533.10");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GENE3D_1));
    }

    @Test
    void findGene3dIdWithoutSpecifyingXrefType() {
        String query = xref("1.10.533.10");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GENE3D_1));
    }

    @Test
    void goExactlyCorrectAccession() {
        String query = query(UniProtField.Search.accession, GO_1);
        query = QueryBuilder.and(query, xref("GO", "GO:0033644"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_1));
    }

    @Test
    void goCaseInSensitiveAccession() {
        String query = query(UniProtField.Search.accession, GO_1);
        query = QueryBuilder.and(query, xref("GO", "go:0033644"));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(GO_1));
    }

    @Test
    void refseqCommonWordFoundInBothEntries() {
        String query = xref("REFSEQ", "*");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder("T1TTT4", "T1TTT5"));
    }

    @Test
    void refseqSearchFindingNothing() {
        String query = xref("REFSEQ", "SUPERMAN");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refseqIDNoVersion() {
        String query = query(UniProtField.Search.accession, REF_SEQ_1);
        query = QueryBuilder.and(query, xref("REFSEQ", "YP_654585"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(REF_SEQ_1));
    }

    @Test
    void refseqIDWithVersion() {
        String query = query(UniProtField.Search.accession, REF_SEQ_1);
        query = QueryBuilder.and(query, xref("REFSEQ", "YP_654585.1"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(REF_SEQ_1));
    }

    @Test
    void refseqDontFindIDWithVersion() {
        String query = query(UniProtField.Search.accession, REF_SEQ_1);
        query = QueryBuilder.and(query, xref("REFSEQ", "YP_654585.2"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refseqDontFindIDWithVersionAgain() {
        String query = query(UniProtField.Search.accession, REF_SEQ_2);
        query = QueryBuilder.and(query, xref("REFSEQ", "NC_008187.1"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void refseqFindIDWithVersion() {
        String query = query(UniProtField.Search.accession, REF_SEQ_2);
        query = QueryBuilder.and(query, xref("REFSEQ", "NC_008187.2"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(REF_SEQ_2));
    }

    @Test
    void refseqFindIDWithNoVersionMultipleMatches() {
        String query = xref("REFSEQ", "NC_008187");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(REF_SEQ_2, REF_SEQ_1));
    }

    @Test
    void emblFindIDWithNoVersionMultiMatch() {
        String query = xref("EMBL", "AY548484");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(EMBL_1, EMBL_2));
    }

    @Test
    void emblFindIDWithNoVersionSingleMatch() {
        String query = xref("EMBL", "BY548484");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(EMBL_3));
    }

    @Test
    void emblDontFindInEntry() {
        String query = query(UniProtField.Search.accession, EMBL_1);
        query = QueryBuilder.and(query, xref("EMBL", "BY548484"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void emblFindInEntry() {
        String query = query(UniProtField.Search.accession, EMBL_3);
        query = QueryBuilder.and(query, xref("EMBL", "BY548484"));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(EMBL_3));
    }

    @Test
    void emblWildCardEntry() {
        String query = xref("EMBL", "*");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(EMBL_1, EMBL_2, EMBL_3));
    }

    @Test
    void emblCount() {
        String query = QueryBuilder.rangeQuery("xref_count_embl", 1, 2);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(EMBL_1, EMBL_2, EMBL_3));
    }

    @Test
    void emblCount2() {
        String query = QueryBuilder.rangeQuery("xref_count_embl", 1, 1);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(EMBL_2, EMBL_3));
    }

    @Test
    void emblCount3() {
        String query = QueryBuilder.rangeQuery("xref_count_embl", 2, 3);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(EMBL_1));
    }

    @Test
    void emblFindProteinIDWithoutVersion() {
        String query = xref("EMBL", "BAT09661");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(EMBL_3));
    }

    @Test
    void emblFindProteinIDWithVersionNoVersion() {
        String query = xref("EMBL", "AAT09661");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(EMBL_1, EMBL_2));
    }

    @Test
    void emblFindProteinIDWithVersion() {
        String query = xref("EMBL", "AAT09661.1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, contains(EMBL_1));
    }

    @Test
    void allergomeFindaccession() {
        String query = xref("ALLERGOME", "10031");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ALLTERGOME_1));
    }

    @Test
    void allergomeFindNothingWrongaccession() {
        String query = xref("ALLERGOME", "12.010");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void ccdsFindCCDSNoVersionaccession() {
        String query = xref("CCDS", "CCDS10001");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(CCDS_1));
    }

    @Test
    void ccdsFindCCDSWithVersionaccession() {
        String query = xref("CCDS", "CCDS10001.1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(CCDS_1));
    }

    @Test
    void ccdsFindNoCCDSWithVersionaccession() {
        String query = xref("CCDS", "CCDS10001.10");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void zfinFindExactSecond() {
        String query = xref("ZFIN", "ZDB-GENE-091204-381");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ZFIN_1));
    }

    @Disabled
    @Test
    void tcdbFindWithPlus() {
        String query = xref("TCDB", "na(+)");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_3));
    }

    @Test
    void tcdbFindId() {
        String query = xref("TCDB", "3.D.6.1.1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_3));
    }

    @Test
    void findTcdbIdWithoutSpecifyingXrefType() {
        String query = xref("3.D.6.1.1");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_3));
    }

    @Disabled
    @Test
    void tcdbFindWithPlusAndMore() {
        String query = xref("TCDB", "na(+) translocating");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_3));
    }

    @Disabled
    @Test
    void tcdbFindBothPutative() {
        String query = xref("TCDB", "putative");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(TCDB_2, TCDB_3));
    }

    @Disabled
    @Test
    void tcdbFindBothThisIsOccurrences() {
        String query = xref("TCDB", "zebra-is");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(TCDB_2, TCDB_3));
    }

    @Disabled
    @Test
    void tcdbFindOnlyOnePutativeInorganic() {
        String query = xref("TCDB", "putative inorganic");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(TCDB_2));
    }

    @Disabled
    @Test
    void tcdbFindSingleThisIsAOccurrence() {
        String query = xref("TCDB", "zebra-feather-brownian");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_2));
    }

    @Disabled
    @Test
    void tcdbFindSingleThisIsAWordOccurrence() {
        String query = xref("TCDB", "zebra-feather-brownian-monster");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(TCDB_2));
    }

    private String xref(String type, String value) {
        return query(UniProtField.Search.xref, type.toLowerCase() + "-" + value);
    }

    private String xref(String value) {
        return query(UniProtField.Search.xref, value);
    }
}
