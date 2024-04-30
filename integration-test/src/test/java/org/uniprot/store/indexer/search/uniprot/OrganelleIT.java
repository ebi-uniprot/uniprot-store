package org.uniprot.store.indexer.search.uniprot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.uniprot.core.uniprotkb.GeneEncodingType.*;
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

/** Tests whether the organelles of a UniProt entry have been indexed correctly */
class OrganelleIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    // Entry 1
    private static final String ACCESSION1 = "Q197F4";
    private static final String ORGANELLE1 = MITOCHONDRION.getName();
    // Entry 2
    private static final String ACCESSION2 = "Q197F5";
    private static final String ORGANELLE_SPECIFIC_NAME1 = "pCP301";
    private static final String ORGANELLE2 = PLASMID.getName() + " " + ORGANELLE_SPECIFIC_NAME1;
    private static final String ORGANELLE_SPECIFIC_NAME2 = "pWR100";
    private static final String ORGANELLE3 = PLASMID.getName() + " " + ORGANELLE_SPECIFIC_NAME2;
    private static final String ORGANELLE_SPECIFIC_NAME3 = "pINV_F6_M1382";
    private static final String ORGANELLE4 = PLASMID.getName() + " " + ORGANELLE_SPECIFIC_NAME3;
    // Entry 3
    private static final String ACCESSION3 = "Q197F6";
    private static final String ORGANELLE5 =
            PLASTID.getName() + "; " + CYANELLE.getName(); // CYANELLE is child of PLASTID
    // Entry 4
    private static final String ACCESSION4 = "Q197F7";
    private static final String ORGANELLE6 =
            PLASTID.getName() + "; " + ORGANELLAR_CHROMATOPHORE.getName();
    ;
    @RegisterExtension static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy =
                UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // Entry 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION1));
        entryProxy.updateEntryObject(LineType.OG, createOGLine(ORGANELLE1));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION2));
        entryProxy.updateEntryObject(LineType.OG, createOGLine(ORGANELLE3, ORGANELLE2, ORGANELLE4));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION3));
        entryProxy.updateEntryObject(LineType.OG, createOGLine(ORGANELLE5));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // Entry 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, ACCESSION4));
        entryProxy.updateEntryObject(LineType.OG, createOGLine(ORGANELLE6));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    private static String createOGLine(String... organelles) {
        StringBuilder line = new StringBuilder("OG   ");

        if (organelles.length > 0) {
            for (int i = 0; i < organelles.length; i++) {
                // if more than one organelle exists the last one gets an and appended to it
                if (organelles.length > 1 && i == (organelles.length - 1)) {
                    line.append("and ");
                }
                line.append(organelles[i]).append(", ");
            }

            line.replace(line.length() - 2, line.length(), ".");
        } else {
            line.append(".");
        }
        return line.toString();
    }

    @Test
    void noMatchesForNonExistentOrganelle() {
        String query = organelle(HYDROGENOSOME.getName());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void organelleFromEntry1MatchesEntry1() {
        String query = organelle(MITOCHONDRION.getName());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION1));
    }

    @Test
    void partialPlasmidSearchMatchesEntry2() {
        String query = organelle(PLASMID.getName());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION2));
    }

    @Test
    void partialPlasmidSpecificNameNotFound() {
        String query = organelle(ORGANELLE_SPECIFIC_NAME2);

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }

    @Test
    void plastidChildSearchMatchesEntry3() {
        String query = organelle(ORGANELLAR_CHROMATOPHORE.getName().toLowerCase());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(ACCESSION4));
    }

    @Test
    void plastidParentSearchMatchesEntry3And4() {
        String query = organelle(PLASTID.name());

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, containsInAnyOrder(ACCESSION3, ACCESSION4));
    }

    String organelle(String name) {
        return query(
                searchEngine.getSearchFieldConfig().getSearchFieldItemByName("organelle"), name);
    }
}
