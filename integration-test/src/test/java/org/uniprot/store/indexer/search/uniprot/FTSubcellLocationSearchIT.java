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

class FTSubcellLocationSearchIT {
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q197B1 = "Q197B1";
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q12345 = "Q12345";
    private static final String Q6GZN7 = "Q6GZN7";
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
                "FT   TRANSMEM        580..604\n"
                        + "FT                   /note=\"Helical\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // --------------
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q197B1));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   TRANSMEM        705..725\n"
                        + "FT                   /note=\"Helical\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"\n"
                        + "FT   TOPO_DOM        726..1070\n"
                        + "FT                   /note=\"Cytoplasmic\"\n"
                        + "FT                   /evidence=\"ECO:0000255\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q12345));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   INTRAMEM        510..547\n"
                        + "FT                   /note=\"Helical\"\n"
                        + "FT                   /evidence=\"ECO:0000250|UniProtKB:F1RAX4\"\n"
                        + "FT   TOPO_DOM        548..667\n"
                        + "FT                   /note=\"Cytoplasmic\"\n"
                        + "FT                   /evidence=\"ECO:0000250|UniProtKB:F1RAX4\"");

        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZN7));
        entryProxy.updateEntryObject(
                LineType.FT,
                "FT   MUTAGEN         15\n"
                        + "FT                   /note=\"T->V: No effect on molecular weight; when associated with V-109 and V-116.\"\n"
                        + "FT                   /evidence=\"ECO:0000269|PubMed:16956885\"\n"
                        + "FT   HELIX           4..13\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:4QOB\"\n"
                        + "FT   HELIX           17..27\n"
                        + "FT                   /evidence=\"ECO:0000244|PDB:4QOB\"");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        searchEngine.printIndexContents();
    }

    @Test
    void transmemFindEntryWith() {
        String query = features(UniprotKBFeatureType.TRANSMEM, "Helical");
        //	query = QueryBuilder.and(query, featureLength(FeatureType.BINDING, 1, 2));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q6GZX4, Q197B1));
        assertThat(retrievedAccessions, not(hasItem(Q12345)));
    }

    @Test
    void transmemFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.TRANSMEM, "Helical");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.TRANSMEM, 20, 23));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasItems(Q197B1));
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, not(hasItem(Q6GZX4)));
    }

    @Test
    void topodomFindTwoEntry() {
        String query = features(UniprotKBFeatureType.TOPO_DOM, "cytoplasmic");
        //	query = QueryBuilder.and(query, featureLength(FeatureType.BINDING, 1, 2));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q197B1, Q12345));
    }

    @Test
    void topodomFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.TOPO_DOM, "cytoplasmic");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.TOPO_DOM, 100, 200));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void intramemFindTwoEntryWithLength() {
        String query = features(UniprotKBFeatureType.INTRAMEM, "helical");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.INTRAMEM, 30, 40));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q12345));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void intramemFindTwoEntryWithLengthOutRange() {
        String query = features(UniprotKBFeatureType.INTRAMEM, "helical");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.INTRAMEM, 40, 50));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, empty());
    }

    @Test
    void mutagenFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.MUTAGEN, "molecular");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.MUTAGEN, 1, 1));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }

    @Test
    void helixFindEntryWithLength() {
        String query = features(UniprotKBFeatureType.HELIX, "*");
        query = QueryBuilder.and(query, featureLength(UniprotKBFeatureType.HELIX, 5, 11));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        System.out.println(retrievedAccessions);
        assertThat(retrievedAccessions, hasItems(Q6GZN7));
        assertThat(retrievedAccessions, not(hasItem(Q197B1)));
    }
}
