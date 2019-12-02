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
import org.uniprot.core.uniprot.feature.FeatureType;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

class FTSequenceSearchIT {

    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q197B1 = "Q197B1";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q12345 = "Q12345";
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
                LineType.FT,
                "FT   VAR_SEQ         234..249\n"
                        + "FT                   /note=\"Missing (in isoform 3)\"\n"
                        + "FT                   /evidence=\"ECO:0000305\"\n"
                        + "FT                   /id=\"VSP_055167\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   VARIANT         177\n"
                        + "FT                   /note=\"R -> Q (in a colorectal cancer sample; somatic mutation; dbSNP:rs374122292)\"\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:16959974\"\n"
                        + "FT                   /id=\"VAR_035897\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   NON_STD         92\n"
                        + "FT                   /note=\"Selenocysteine\"\n"
                        + "FT                   /evidence=\"ECO:0000250\"\n"
                        + "FT   NON_TER         1\n"
                        + "FT                   /evidence=\"ECO:0000303|PubMed:17963685\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   NON_CONS        15..16\n"
                        + "FT                   /evidence=\"ECO:0000305\"\n"
                        + "FT                   /id=\"PRO_0000027671\"\n"
                        + "FT   CONFLICT        758\n"
                        + "FT                   /note=\"P -> A (in Ref. 1; CAA57732)\"\n"
                        + "FT                   /evidence=\"ECO:0000305\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6V4H0));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   UNSURE          44\n"
                        + "FT                   /evidence=\"ECO:0000269|Ref.1\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    void varSeqFindEntryWithEvidenceLength() {
        String query = features(FeatureType.VAR_SEQ, "isoform");
        query = QueryBuilder.and(query, featureLength(FeatureType.VAR_SEQ, 10, 20));
        String evidence = "ECO_0000305";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.VAR_SEQ, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZX4));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void variantFindEntryWithEvidenceLength() {
        String query = features(FeatureType.VARIANT, "colorectal");
        query = QueryBuilder.and(query, featureLength(FeatureType.VARIANT, 1, 1));
        String evidence = "ECO_0000269";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.VARIANT, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void variantsFindEntryWithLengthAndEvidence() {
        String query = query(UniProtField.Search.ft_variants, "colorectal");
        query =
                QueryBuilder.and(
                        query,
                        QueryBuilder.rangeQuery(UniProtField.Search.ftlen_variants.name(), 1, 21));
        String evidence = "ECO_0000269";
        query = QueryBuilder.and(query, query(UniProtField.Search.ftev_variants, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void nonStdFindEntryWithEvidenceLength() {
        String query = features(FeatureType.NON_STD, "selenocysteine");
        query = QueryBuilder.and(query, featureLength(FeatureType.NON_STD, 1, 1));
        String evidence = "ECO_0000250";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_STD, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void nonTerFindEntryWithEvidenceLength() {
        String query = features(FeatureType.NON_TER, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.NON_TER, 1, 1));
        String evidence = "ECO_0000303";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_TER, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void nonConsFindEntryWithEvidenceLength() {
        String query = features(FeatureType.NON_CONS, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.NON_CONS, 1, 2));
        String evidence = "ECO_0000305";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.NON_CONS, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void conflictFindEntryWithEvidenceLength() {
        String query = features(FeatureType.CONFLICT, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.CONFLICT, 1, 2));
        String evidence = "ECO_0000305";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.CONFLICT, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void unsureFindEntryWithEvidenceLength() {
        String query = features(FeatureType.UNSURE, "*");
        query = QueryBuilder.and(query, featureLength(FeatureType.UNSURE, 1, 2));
        String evidence = "ECO_0000269";
        query = QueryBuilder.and(query, featureEvidence(FeatureType.UNSURE, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6V4H0));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void positionFindEntryWithLengthAndEvidence() {
        String query = query(UniProtField.Search.ft_positional, "colorectal");
        query =
                QueryBuilder.and(
                        query,
                        QueryBuilder.rangeQuery(
                                UniProtField.Search.ftlen_positional.name(), 1, 21));
        String evidence = "ECO_0000269";
        query = QueryBuilder.and(query, query(UniProtField.Search.ftev_positional, evidence));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }
}
