package uk.ac.ebi.uniprot.indexer.search.uniprot;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.function.Function;

import uk.ac.ebi.uniprot.flatfile.parser.ffwriter.LineType;
import uk.ac.ebi.uniprot.indexer.document.field.QueryBuilder;
import uk.ac.ebi.uniprot.indexer.document.field.UniProtField;
import uk.ac.ebi.uniprot.indexer.search.DocFieldTransformer;
import static uk.ac.ebi.uniprot.indexer.search.DocFieldTransformer.fieldTransformer;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsNot.not;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.TestUtils.convertToUniProtEntry;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.BasicCIAnalysisSearchIT.FieldType.TypeFunctions.STRING_FUNCTION;
import static uk.ac.ebi.uniprot.indexer.search.uniprot.BasicCIAnalysisSearchIT.FieldType.TypeFunctions.STRING_LIST_FUNCTION;


/**
 * This class tests the edge cases of the {@code basic_ci} field type defined in UniProt's schema.xml.
 * <p>
 * For all fields that use {@code basic_ci}, add a corresponding {@link FieldType} and add it to the {@code @Parameters}
 * collection of fields to test. This ensures all fields that should use this field type, really do.
 * <p>
 * Created 02/07/18
 *
 * @author Edd
 */
@RunWith(Parameterized.class)
public class BasicCIAnalysisSearchIT {
    @ClassRule
    public static final UniProtSearchEngine searchEngine = new UniProtSearchEngine();
    private static final String RESOURCE_ENTRY_PATH = "/it/uniprot";
    private static final List<String> RESOURCE_ENTRIES_TO_STORE =
            asList("P0A377.43", "P51587", "Q6GZV4.23", "Q197D8.25", "Q197F8.16");
    private static UniProtEntryObjectProxy entryProxy;
    private static int accessionId = 0;
    private final FieldType field;
    private List<String> tempSavedEntries = new ArrayList<>();
    public static final String ACC_LINE = "AC   %s;";

    public BasicCIAnalysisSearchIT(FieldType field) {
        this.field = field;
    }

    @Parameterized.Parameters(name = "field = {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {FieldType.accession_id},
                {FieldType.mnemonic},
                {FieldType.gene_exact},
                {FieldType.existence},
                {FieldType.sec_acc}
        });
    }

    @BeforeClass
    public static void populateIndexWithTestData() throws IOException{
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

    // phrases (even though it's treated as a single token with the basic analyser)
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

    // non-phrase queries
    @Test
    public void canFindAccessionLikeValue() {
        String accession = newAccession();
        String fieldValue = "P12345";
        String query =fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void canFindComplexExactValue() {
        String accession = newAccession();
        String fieldValue = "aA12-3a-a44b-a4/VA,RV_IND64_vel4_019";
        String query = fieldQuery(field.name(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound();
    }

    @Test
    public void cannotUseMiddlePartsOfValueToFindValueWithUnderScores() {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.name(), "IND64_vel4");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canNotBeFound();
    }

    @Test
    public void canFindValueThatIsOnlyANumber() {
        String accession = newAccession();
        String fieldValue = "62";
        String query = fieldQuery(field.name(), fieldValue);

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
            String fieldValue = "hello" + toEscape + "world";
            String query = fieldQuery(field.name(), fieldValue);
            System.out.println(query);
            new EntryCheck()
                    .withAccession(accession)
                    .withFieldValue(fieldValue)
                    .usingQuery(query)
                    .canBeFound();

            searchEngine.removeEntry(accession);
        }
    }

    private static void ensureInitialEntriesWereSaved() {
  
        String query =UniProtField.Search.accession_id.name() +":*" ;


        QueryResponse response = searchEngine.getQueryResponse(query);

        List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
        assertThat(retrievedAccessions, hasSize(RESOURCE_ENTRIES_TO_STORE.size()));
    }

    private String fieldQuery(String field, String fieldValue) {
        String result = QueryBuilder.query(field, fieldValue, false, false);
        
        return result;
 
    }

    private String fieldPhraseQuery(String field, String fieldValue) {
    	return QueryBuilder.query(field, fieldValue,true, false);
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
        accession_id(STRING_FUNCTION),
        gene_exact(STRING_LIST_FUNCTION),
        existence(STRING_FUNCTION),
        mnemonic(STRING_FUNCTION),
        sec_acc(STRING_LIST_FUNCTION);

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
            List<String> retrievedAccessions = findAccessions();

            assertThat(retrievedAccessions, contains(toFind()));
        }

        void canNotBeFound() {
            List<String> retrievedAccessions = findAccessions();
            assertThat(retrievedAccessions, not(contains(toFind())));
        }

        private String toFind() {
            String toFind;
            if (field.name().equals("accession_id")) {
                toFind = fieldValue;
            } else {
                toFind = accession;
            }
            return toFind;
        }

        private List<String> findAccessions() {
            index(accession, fieldValue);
            QueryResponse response = searchEngine.getQueryResponse(query);

            return searchEngine.getIdentifiers(response);
        }
    }
}
