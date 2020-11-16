package org.uniprot.store.indexer.search.uniprot;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.uniprot.store.indexer.search.DocFieldTransformer.fieldTransformer;
import static org.uniprot.store.indexer.search.uniprot.FullCIAnalysisSearchIT.FieldType.TypeFunctions.STRING_LIST_FUNCTION;
import static org.uniprot.store.indexer.search.uniprot.IdentifierSearchIT.ACC_LINE;
import static org.uniprot.store.indexer.search.uniprot.TestUtils.convertToUniProtEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.uniprot.core.flatfile.writer.LineType;
import org.uniprot.store.indexer.search.DocFieldTransformer;
import org.uniprot.store.search.field.QueryBuilder;

/**
 * This class tests the edge cases of the {@code full_ci} field type defined in UniProt's
 * schema.xml.
 *
 * <p>For all fields that use {@code full_ci}, add a corresponding {@link FieldType} and add it to
 * the {@code @Parameters} collection of fields to test. This ensures all fields that should use
 * this field type, really do.
 *
 * <p>Created 02/07/18
 *
 * @author Edd
 */
class FullCIAnalysisSearchIT {
    @RegisterExtension static final UniProtSearchEngine searchEngine = new UniProtSearchEngine();
    private static final String RESOURCE_ENTRY_PATH = "/it/uniprot";
    private static final List<String> RESOURCE_ENTRIES_TO_STORE =
            asList("P0A377.43", "P51587", "Q6GZV4.23", "Q197D8.25", "Q197F8.16");
    private static UniProtEntryObjectProxy entryProxy;
    private static int accessionId = 0;
    private final List<String> tempSavedEntries = new ArrayList<>();

    @BeforeAll
    static void populateIndexWithTestData() throws IOException {
        for (String entryToStore : RESOURCE_ENTRIES_TO_STORE) {
            InputStream resourceAsStream =
                    TestUtils.getResourceAsStream(
                            RESOURCE_ENTRY_PATH + "/" + entryToStore + ".dat");
            entryProxy = UniProtEntryObjectProxy.createEntryFromInputStream(resourceAsStream);

            searchEngine.indexEntry(convertToUniProtEntry(entryProxy));
        }

        searchEngine.printIndexContents();
        ensureInitialEntriesWereSaved();
    }

    @AfterEach
    void after() {
        cleanTempEntries();
    }

    // phrases
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindSimpleExactPhrase(FieldType field) {
        String accession = newAccession();
        String fieldValue = "hello world";

        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactPhraseContainingCommasNumbersBracesAndSlashes(FieldType field) {
        String accession = newAccession();
        String fieldValue =
                "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindPhraseContainingCommasNumbersBracesAndSlashes(FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Influenza A, virus (strain A/Goose/Guangdong/1/1996 H5N1";
        String queryFieldValue = "strain A/Goose/Guangdong/1/1996 H5N1";

        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindPhraseContainingExactCamelCaseQuery(FieldType field) {
        String accession = newAccession();

        String fieldValue = "Keratin, type I microfibrillar 48 kDa, component 8C-1";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindPhraseContainingCamelCaseQuery(FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Keratin, type I microfibrillar 48 kDa, component 8C-1";
        String queryFieldValue = "Keratin, type I microfibrillar 48 kDa, component";

        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindProblematicBaseCasePhraseQuery(FieldType field) {
        String accession = newAccession();
        String fieldValue = "1a b2";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindWordWithEqualsQuery(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Note=Translated";
        String queryFieldValue = "Translated";
        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // single word
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindSingleExactValue(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindComplexBracketeProteinName(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindComplexBracketeProteinNameViaBracketedPart(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String queryFieldValue = "(Ms(2)io(6)a)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindComplexBracketeProteinNameViaLastPart(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String queryFieldValue = "hydroxylase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindSingleExactValueWithTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate;";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindSingleExactValueWithoutTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindSingleValueWithTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate";
        String queryFieldValue = "Aspartate;";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-word
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactValue(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindComplexFieldUsingExactValue(FieldType field) {
        String accession = newAccession();
        String fieldValue = "(+)-car-3-ene synthase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindComplexFieldUsingPartialValue(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "(+)-car-3-ene synthase";
        String queryFieldValue = "3-ene synthase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactValueWithTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactValueWithoutTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canBlahBlee(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "aminotransferase Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueWithTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindMultiWordValueGivenOneWord(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindMultiWordValueGivenOneWordWhenTheresATerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-word terms
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactValueContainingComma(FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase, mitochondrial";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueContainingCommaWithoutTerminatingSemiColon(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "L-topaquinone(1-) residue [79027]";
        String queryFieldValue = "L-topaquinone(1-)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindMultiWordValueContainingCommaGivenOneWordWhenTheresATerminatingSemiColon(
            FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate, aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-words with slashes
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindMultiWordValueWithSlash(FieldType field) {
        String accession = newAccession();
        String fieldValue =
                "Influenza A virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // words containing special chars (just using '_' in tests, but applies to all non-word chars)
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueWithUnderScore(FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void caseInsensitiveFindValueWithUnderScores(FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), fieldValue.toLowerCase());

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canUseFirstPartOfValueToFindValueWithUnderScores(FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "VARV");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canUseMiddlePartOfValueToFindValueWithUnderScores(FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "vel4");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canUseMiddlePartsOfValueToFindValueWithUnderScores(FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "IND64_vel4");

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueThatIsOnlyANumber(FieldType field) {
        String accession = newAccession();
        String fieldValue = "62";
        String query = fieldQuery(field.getQueryField(), fieldValue);
        System.out.println(query);
        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValuesContainingSpecialChars(FieldType field) {
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
            String fieldValue = "hi" + toEscape + "world";
            String query = fieldQuery(field.getQueryField(), fieldValue);

            new EntryCheck()
                    .withAccession(accession)
                    .withFieldValue(fieldValue)
                    .usingQuery(query)
                    .canBeFound(field);

            searchEngine.removeEntry(accession);
        }
    }

    @Disabled
    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueViaAlternativeSpellingFromSynonymList(FieldType field) {
        // ensure synonyms in are used:
        //
        // uniprot-data-services/data-service-deployments/src/main/distros/solr-conf/homes/uniprot-cores/uniprot/conf/synonyms.txt
        String accession = newAccession();
        String indexFieldValue = "hemoglobin tumor";
        String queryFieldValue = "haemoglobin tumour";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindValueViaWithoutPossessive(FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "this was bill's";
        String queryFieldValue = "this was bill";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactPhraseWithDash(FieldType field) {
        String accession = newAccession();
        String fieldValue = "LAGE-1, a new gene with tumor specificity.";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactPhraseWithDash2(FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String queryFieldValue = "Cys-tRNA(Pro)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FieldType.class)
    void canFindExactPhraseWithDash3(FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String queryFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
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

    private void index(String accession, String fieldValue, FieldType field) {
        DocFieldTransformer docFieldTransformer =
                fieldTransformer(field.name(), field.getType().apply(fieldValue));
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
        gene(STRING_LIST_FUNCTION, "protgene_default"),
        organism_name(STRING_LIST_FUNCTION, "organism_name"),
        taxonomy_name(STRING_LIST_FUNCTION, "taxonomy_name"),
        virus_host_name(STRING_LIST_FUNCTION, "virus_host_name"),
        protein_name(STRING_LIST_FUNCTION, "protgene_default");

        private final Function<String, ?> field;

        public String getQueryField() {
            return queryField;
        }

        private final String queryField;

        FieldType(Function<String, ?> field, String queryField) {
            this.field = field;
            this.queryField = queryField;
        }

        public Function<String, ?> getType() {
            return this.field;
        }

        static class TypeFunctions {
            static final Function<String, List<String>> STRING_LIST_FUNCTION =
                    Collections::singletonList;
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

        void canBeFound(FieldType field) {
            index(accession, fieldValue, field);
            QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, contains(accession));
        }
    }
}
