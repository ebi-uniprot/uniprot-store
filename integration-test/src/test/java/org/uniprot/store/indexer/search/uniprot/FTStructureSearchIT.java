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
import org.uniprot.core.uniprotkb.feature.FeatureType;
import org.uniprot.store.search.field.QueryBuilder;

class FTStructureSearchIT {

    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q197B1 = "Q197B1";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/Q197D8.25.dat";
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
        entryProxy.updateEntryObject(LineType.DR, "DR   PDB; 3SR9; X-ray; 2.40 A; A=1326-1901.");
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   HELIX           428..430\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:2A8B\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
        entryProxy.updateEntryObject(LineType.DR, "DR   EMBL; BC083188; AAH83188.1; -; mRNA.");
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   STRAND          487..492\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:2A8B\"\n"
                        + "FT   STRAND          494..499\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:2A8B\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(LineType.DR, "DR   EMBL; BC083188; AAH83188.1; -; mRNA.");
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   TURN            1476..1478\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:4C6F\"\n"
                        + "FT   TURN            1480..1482\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:4C6F\"\n"
                        + "FT   HELIX           1485..1494\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:4C6F\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        searchEngine.printIndexContents();
    }

    @Test
    void d3StructureFindEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("d3structure"),
                        "true");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void note3StructureFindEntry() {
        String query =
                query(
                        searchEngine.getSearchFieldConfig().getSearchFieldItemByName("d3structure"),
                        "false");
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void strandFindEntryWithEvidenceLength() {
        String query = features(FeatureType.STRAND, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.STRAND, 1, 25));
        String evidence = "ECO_0000244";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.STRAND, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void turnFindEntryWithEvidenceLength() {
        String query = features(FeatureType.TURN, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.TURN, 1, 25));
        String evidence = "ECO_0000244";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.TURN, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void helixFindEntryWithEvidenceLength() {
        String query = features(FeatureType.HELIX, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.HELIX, 9, 25));
        String evidence = "ECO_0000244";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.HELIX, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void helixFindTwoEntriesWithEvidenceLength() {
        String query = features(FeatureType.HELIX, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.HELIX, 1, 25));
        String evidence = "ECO_0000244";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.HELIX, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345, Q6GZX4));
    }

    @Test
    void secstructFindTwoEntriesWithEvidenceLength() {
        String query =
                query(
                        searchEngine
                                .getSearchFieldConfig()
                                .getSearchFieldItemByName("ft_secstruct"),
                        "*");
        query =
                QueryBuilder.and(
                        query,
                        QueryBuilder.rangeQuery(
                                searchEngine
                                        .getSearchFieldConfig()
                                        .getSearchFieldItemByName("ftlen_secstruct")
                                        .getFieldName(),
                                1,
                                25));
        String evidence = "ECO_0000244";
        query =
                QueryBuilder.and(
                        query,
                        query(
                                searchEngine
                                        .getSearchFieldConfig()
                                        .getSearchFieldItemByName("ftev_secstruct"),
                                evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345, Q6GZX4, Q197B1));
    }
}
