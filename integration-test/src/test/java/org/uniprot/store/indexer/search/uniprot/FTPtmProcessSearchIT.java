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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprotkb.feature.UniprotKBFeatureType;
import org.uniprot.store.search.field.QueryBuilder;

class FTPtmProcessSearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q197B1 = "Q197B1";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q12345 = "Q12345";
    private static final String Q6GZN7 = "Q6GZN7";
    private static final String Q6V4H0 = "Q6V4H0";
    private static final String P48347 = "P48347";
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
                LineType.FT,
                "FT   MOD_RES         853\n"
                        + "FT                   /note=\"Phosphoserine\"\n"
                        + "FT                   /evidence=\"ECO:0000244|PubMed:19690332, ECO:0000244|PubMed:23186163\"\n"
                        + "FT   CHAIN           41..387\n"
                        + "FT                   /note=\"Protein disulfide isomerase pTAC5, chloroplastic\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"\n"
                        + "FT                   /id=\"PRO_0000441697\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   CARBOHYD        55\n"
                        + "FT                   /note=\"S-linked (Hex...) cysteine\"\n"
                        + "FT                   /evidence=\"ECO:0000250\"\n"
                        + "FT   CARBOHYD        583\n"
                        + "FT                   /note=\"N-linked (GlcNAc...) asparagine\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   LIPID           200\n"
                        + "FT                   /note=\"S-geranylgeranyl cysteine\"\n"
                        + "FT                   /evidence=\"ECO:0000250\"\n"
                        + "FT   DISULFID        51..177\n"
                        + "FT                   /note=\"Reversible\"\n"
                        + "FT                   /evidence=\"ECO:0000250|UniProtKB:Q84MC7\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   CROSSLNK        13\n"
                        + "FT                   /note=\"Glycyl lysine isopeptide (Lys-Gly) (interchain with G-Cter in ubiquitin)\"\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:18716620\"\n"
                        + "FT   CROSSLNK        289\n"
                        + "FT                   /note=\"Glycyl lysine isopeptide (Lys-Gly) (interchain with G-Cter in ubiquitin)\"\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:18716620\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   INIT_MET        1\n"
                        + "FT                   /note=\"Removed\"\n"
                        + "FT                   /evidence=\"ECO:0000244|PubMed:22814378\"\n"
                        + "FT   PEPTIDE         311..320\n"
                        + "FT                   /note=\"Linker peptide\"\n"
                        + "FT                   /evidence=\"ECO:0000305|PubMed:10785398\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, P48347));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   SIGNAL          1..19\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:2765556\"\n"
                        + "FT   PROPEP          17..27\n"
                        + "FT                   /note=\"Activation peptide\"\n"
                        + "FT                   /evidence=\"ECO:0000250\"\n"
                        + "FT                   /id=\"PRO_0000027671\"\n"
                        + "FT   TRANSIT         1..20\n"
                        + "FT                   /note=\"Chloroplast\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void modResFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.MOD_RES, "phosphoserine");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.MOD_RES, 1, 1));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void lipidFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.LIPID, "cysteine");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.LIPID, 1, 1));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void carbohydFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.CARBOHYD, "cysteine");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.CARBOHYD, 1, 1));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void disulfidFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.DISULFID, "reversible");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.DISULFID, 100, 150));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void crosslinkFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.CROSSLNK, "lysine");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.CROSSLNK, 1, 1));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void chainFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.CHAIN, "disulfide");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.CHAIN, 200, 400));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void initMetFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.INIT_MET, "removed");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.INIT_MET, 1, 1));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void peptideFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.PEPTIDE, "peptide");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.PEPTIDE, 10, 20));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void signalFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.SIGNAL, "*");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.SIGNAL, 10, 20));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(P48347));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void propepFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.PROPEP, "peptide");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.PROPEP, 5, 20));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(P48347));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void transitFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.TRANSIT, "chloroplast");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.TRANSIT, 5, 20));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(P48347));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void moleculeProcessFindTwoEntry() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("ft_molecule_processing"),
                        "peptide");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(P48347, Q6V4H0));
    }

    @Test
    void moleculeProcessFindTwoEntryWithLength() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("ft_molecule_processing"),
                        "peptide");
        query =
                QueryBuilder.and(
                        query,
                        QueryBuilder.rangeQuery(
                                searchEngine
                                        .getSearchFieldConfig()
                                        .getSearchFieldItemByName("ftlen_molecule_processing")
                                        .getFieldName(),
                                9,
                                10));

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
        assertThat(retrievedAccessions, not(hasItem(P48347)));
    }
}
