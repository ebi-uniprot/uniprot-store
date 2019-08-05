package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.core.uniprot.ProteinExistence;
import org.uniprot.store.search.field.QueryBuilder;
import org.uniprot.store.search.field.UniProtField;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * Tests if the protein existence search is working correctly
 */
public class ProteinExistenceSearchIT {
    private static final String UNIPROT_FLAT_FILE_ENTRY_PATH = "/it/uniprot/P0A377.43.dat";
    private static final String Q6GZX1 = "Q6GZX1";
    private static final String Q6GZX2 = "Q6GZX2";
    private static final String Q6GZX3 = "Q6GZX3";
    private static final String Q6GZX4 = "Q6GZX4";
    private static final String Q6GZX5 = "Q6GZX5";
    @ClassRule
    public static UniProtSearchEngine searchEngine = new UniProtSearchEngine();

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        // a test entry object that can be modified and added to index
        InputStream resourceAsStream = TestUtils.getResourceAsStream(UNIPROT_FLAT_FILE_ENTRY_PATH);
        UniProtEntryObjectProxy entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

        // proteinExistence 1
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX1));
        entryProxy.updateEntryObject(LineType.PE, "PE   1: Evidence at protein level;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        // proteinExistence 2
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX2));
        entryProxy.updateEntryObject(LineType.PE, "PE   2: Evidence at transcript level;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        // proteinExistence 3
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX3));
        entryProxy.updateEntryObject(LineType.PE, "PE   3: Inferred from homology;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        // proteinExistence 4
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX4));
        entryProxy.updateEntryObject(LineType.PE, "PE   4: Predicted;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        
        // proteinExistence 5
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, Q6GZX5));
        entryProxy.updateEntryObject(LineType.PE, "PE   5: Uncertain;");
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy));

        searchEngine.printIndexContents();
    }

    @Test
    public void peLevelProtein() {
        String query = proteinExistence(ProteinExistence.PROTEIN_LEVEL);
        QueryResponse response = searchEngine.getQueryResponse(query);
        System.out.println(query);
        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX1));
    }

    @Test
    public void peLevelTranscript() {
        String query = proteinExistence(ProteinExistence.TRANSCRIPT_LEVEL);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX2));
    }

    @Test
    public void peLevelHomology() {
        String query = proteinExistence(ProteinExistence.HOMOLOGY);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX3));
    }

    @Test
    public void peLevelPredicted() {
        String query = proteinExistence(ProteinExistence.PREDICTED);
        System.out.println(query);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX4));
    }

    @Test
    public void peLevelUncertain() {
        String query = proteinExistence(ProteinExistence.UNCERTAIN);
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX5));
    }

    @Test
    public void peLevelUncertainWithAcc() {
    	String query = query(UniProtField.Search.accession, Q6GZX5);
    	query =QueryBuilder.and(query, proteinExistence(ProteinExistence.UNCERTAIN));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, contains(Q6GZX5));
    }

    @Test
    public void peLevelFindNothingWithUncertainWithAcc() {
    	String query = query(UniProtField.Search.accession, Q6GZX4);
    	query =QueryBuilder.and(query, proteinExistence(ProteinExistence.UNCERTAIN));
        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, is(empty()));
    }
    String proteinExistence(ProteinExistence proteinExistence) {
        return query(UniProtField.Search.existence, proteinExistence.name());
    }
}
