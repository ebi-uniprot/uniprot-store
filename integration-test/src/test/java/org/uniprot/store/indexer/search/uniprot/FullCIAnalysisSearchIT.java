package org.uniprot.store.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.indexer.search.DocFieldTransformer;
import org.uniprot.store.search.field.QueryBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.uniprot.store.indexer.search.DocFieldTransformer.*;
import static org.uniprot.store.indexer.search.uniprot.FullCIAnalysisSearchIT.FieldType.TypeFunctions.STRING_FUNCTION;
import static org.uniprot.store.indexer.search.uniprot.FullCIAnalysisSearchIT.FieldType.TypeFunctions.STRING_LIST_FUNCTION;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.*;

/**
 * This class tests the edge cases of the {@code full_ci} field type defined in UniProt's schema.xml.
 * <p>
 * For all fields that use {@code full_ci}, add a corresponding {@link FieldType} and add it to the {@code @Parameters}
 * collection of fields to test. This ensures all fields that should use this field type, really do.
 * <p>
 * Created 02/07/18
 *
 * @author Edd
 */
@RunWith(Parameterized.class)
public class FullCIAnalysisSearchIT {
    @ClassRule
    public static final UniProtSearchEngine searchEngine = new UniProtSearchEngine();
    private static final String RESOURCE_ENTRY_PATH = "/it/uniprot";
    private static final List<String> RESOURCE_ENTRIES_TO_STORE =
            asList("P0A377.43", "P51587", "Q6GZV4.23", "Q197D8.25", "Q197F8.16");
    private static UniProtEntryObjectProxy entryProxy;
    private static int accessionId = 0;
    private final FieldType field;
    private List<String> tempSavedEntries = new ArrayList<>();

    public FullCIAnalysisSearchIT(FieldType field) {
        this.field = field;
    }

    @Parameterized.Parameters(name = "field = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {FieldType.gene},
                {FieldType.organism_name},
                {FieldType.host_name},
                {FieldType.taxonomy_name},
                {FieldType.name}
        });
    }

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException {
        for (String entryToStore : RESOURCE_ENTRIES_TO_STORE) {
            InputStream resourceAsStream = TestUtils
                    .getResourceAsStream(RESOURCE_ENTRY_PATH + "/" + entryToStore + ".dat");
            entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        searchEngine.printIndexContents();
        ensureInitialEntriesWereSaved();
    }

    @After
    public void after() {
        cleanTempEntries();
    }

    // phrases
    @Test
    public void canFindSimpleExactPhrase() {
        String accession = newAccession();
        String fieldValue = "hello world";

        String query = fieldPhraseQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    @Ignore
    public void canFindExactPhraseContainingCommasNumbersBracesAndSlashes() {
        String accession = newAccession();
        String fieldValue = "Influenza A, virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)";
        String query = fieldPhraseQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    @Ignore
    public void canFindPhraseContainingCommasNumbersBracesAndSlashes() {
        String accession = newAccession();

        String indexFieldValue = "Influenza A, virus (strain A/Goose/Guangdong/1/1996 H5N1";
        String queryFieldValue = "strain A/Goose/Guangdong/1/1996 H5N1";

        String query = fieldPhraseQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindProblematicBaseCasePhraseQuery() {
        String accession = newAccession();
        String fieldValue = "1a b2";
        String query = fieldPhraseQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindWordWithEqualsQuery() {
        String accession = newAccession();
        String indexFieldValue = "Note=Translated";
        String queryFieldValue = "Translated";
        String query = fieldPhraseQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    // single word
    @Test
    public void canFindSingleExactValue() {
        String accession = newAccession();
        String fieldValue = "Aspartate";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindSingleExactValueWithTerminatingSemiColon() {
        String accession = newAccession();
        String fieldValue = "Aspartate;";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindSingleExactValueWithoutTerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindSingleValueWithTerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate";
        String queryFieldValue = "Aspartate;";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    // multi-word
    @Test
    public void canFindExactValue() {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindComplexFieldUsingExactValue() {
        String accession = newAccession();
        String fieldValue = "(+)-car-3-ene synthase";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindComplexFieldUsingPartialValue() {
        String accession = newAccession();
        String indexFieldValue = "(+)-car-3-ene synthase";
        String queryFieldValue = "3-ene synthase";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindExactValueWithTerminatingSemiColon() {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindExactValueWithoutTerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canBlahBlee() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "aminotransferase Aspartate";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindValueWithTerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindMultiWordValueGivenOneWord() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindMultiWordValueGivenOneWordWhenTheresATerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    // multi-word terms
    @Test
    public void canFindExactValueContainingComma() {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase, mitochondrial";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindValueContainingCommaWithoutTerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "L-topaquinone(1-) residue [79027]";
        String queryFieldValue = "L-topaquinone(1-)";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindMultiWordValueContainingCommaGivenOneWordWhenTheresATerminatingSemiColon() {
        String accession = newAccession();
        String indexFieldValue = "Aspartate, aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    // multi-words with slashes
    @Test
    public void canFindMultiWordValueWithSlash() {
        String accession = newAccession();
        String fieldValue = "Influenza A virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    // words containing special chars (just using '_' in tests, but applies to all non-word chars)
    @Test
    public void canFindValueWithUnderScore() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void caseInsensitiveFindValueWithUnderScores() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), fieldValue.toLowerCase());

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canUseFirstPartOfValueToFindValueWithUnderScores() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), "VARV");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canUseMiddlePartOfValueToFindValueWithUnderScores() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), "vel4");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canUseMiddlePartsOfValueToFindValueWithUnderScores() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), "IND64_vel4");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindValueThatIsOnlyANumber() {
        String accession = newAccession();
        String fieldValue = "62";
        String query = fieldQuery(field.name(), fieldValue);
        System.out.println(query);
        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindValuesContainingSpecialChars() {
        List<String> valuesThatRequireEscaping = asList("+", "-", "&", "|", "!", "(", ")", "{EVIDENCE}", "[", "]", "^", "\"", "~", "?", ":", "/");

        for (String toEscape : valuesThatRequireEscaping) {
            String accession = newAccession();
            String fieldValue = "hi" + toEscape + "world";
            String query = fieldQuery(field.name(), fieldValue);

            new EntryCheck()
                    .withAccession(accession)
                    .withFieldValue(fieldValue)
                    .usingQuery(query)
                    .canBeFound();

            searchEngine.removeEntry(accession);
        }
    }

    @Test @Ignore
    public void canFindValueViaAlternativeSpellingFromSynonymList() {
        // ensure synonyms in are used:
        //    uniprot-data-services/data-service-deployments/src/main/distros/solr-conf/homes/uniprot-cores/uniprot/conf/synonyms.txt
        String accession = newAccession();
        String indexFieldValue = "hemoglobin tumor";
        String queryFieldValue = "haemoglobin tumour";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindValueViaWithoutPossessive() {
        String accession = newAccession();
        String indexFieldValue = "this was bill's";
        String queryFieldValue = "this was bill";
        String query = fieldQuery(field.name(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    private static void ensureInitialEntriesWereSaved() {
        String query = QueryBuilder.query("accession_id", "*");

        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasSize(RESOURCE_ENTRIES_TO_STORE.size()));
    }

    private String fieldQuery(String field, String fieldValue) {
        return QueryBuilder.query(field, fieldValue);
    }

    private String fieldPhraseQuery(String field, String fieldValue) {
        return QueryBuilder.query(field, fieldValue, true, false);
    }

    private void index(String accession, String fieldValue) {
        DocFieldTransformer docFieldTransformer = fieldTransformer(field.name(), field.getType().apply(fieldValue));
        tempSavedEntries.add(accession);
        entryProxy.updateEntryObject(LineType.AC, String.format(ACC_LINE, accession));
        searchEngine.indexEntry(convertToUniProtEntry(entryProxy), docFieldTransformer);
    }

    private void cleanTempEntries() {
        tempSavedEntries.forEach(searchEngine::removeEntry);
    }

    private String newAccession() {
        return "P" + String.format("%05d", accessionId++);
    }

    enum FieldType {
        gene(STRING_LIST_FUNCTION),
        organism_name(STRING_LIST_FUNCTION),
        taxonomy_name(STRING_LIST_FUNCTION),
        host_name(STRING_LIST_FUNCTION),
        existence(STRING_FUNCTION),
        name(STRING_LIST_FUNCTION);

        private Function<String, ?> field;

        FieldType(Function<String, ?> field) {
            this.field = field;
        }

        public Function<String, ?> getType() {
            return this.field;
        }

        static class TypeFunctions {
            static final Function<String, List<String>> STRING_LIST_FUNCTION = Collections::singletonList;
            static final Function<String, String> STRING_FUNCTION = s -> s;
        }
    }

    class EntryCheck {
        String accession;
        String fieldValue;
        String query;

        EntryCheck withAccession(String accession) {
            this.accession = accession;
            return this;
        }

        EntryCheck withFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
            return this;
        }

        EntryCheck usingQuery(String query) {
            this.query = query;
            return this;
        }

        void canBeFound() {
            index(accession, fieldValue);
            QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, contains(accession));
        }
    }
}
