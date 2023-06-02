package org.uniprot.store.indexer.search.uniprot;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.uniprot.store.indexer.search.DocFieldTransformer.fieldTransformer;
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
 * <p>For all fields that use {@code full_ci_split_numeric}, add a corresponding {@link
 * FullCIAnalysisSearchIT.FieldType} and add it to the {@code @Parameters} collection of fields to
 * test. This ensures all fields that should use this field type, really do.
 *
 * <p>Created 12/11/21
 *
 * @author lgonzales
 */
public class FullCISplitNumericAnalysisSearchIT {
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

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindWhenLetterNumberBoundariesExist(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "SAUSAGE1-A complex subunit BRCC36";
        String queryFieldValue = "sausage";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // phrases
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindSimpleExactPhrase(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "hello world";

        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactPhraseContainingCommasNumbersBracesAndSlashes(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindPhraseContainingCommasNumbersBracesAndSlashes(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Influenza A, virus (strain A/Goose/Guangdong/1/1996 H5N1";
        String queryFieldValue = "strain A/Goose/Guangdong/1/1996 H5N1";

        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindPhraseContainingExactCamelCaseQuery(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String fieldValue = "Keratin, type I microfibrillar 48 kDa, component 8C-1";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindPhraseContainingCamelCaseQuery(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Keratin, type I microfibrillar 48 kDa, component 8C-1";
        String queryFieldValue = "Keratin, type I microfibrillar 48 kDa, component";

        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindProblematicBaseCasePhraseQuery(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "1a b2";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindWordWithEqualsQuery(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Note=Translated";
        String queryFieldValue = "Translated";
        String query = fieldPhraseQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // single word
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindSingleExactValue(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindComplexBracketeProteinName(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindComplexBracketeProteinNameViaBracketedPart(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String queryFieldValue = "(Ms(2)io(6)a)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindComplexBracketeProteinNameViaLastPart(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Putative tRNA-(Ms(2)io(6)a)-hydroxylase";
        String queryFieldValue = "hydroxylase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindSingleExactValueWithTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate;";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindSingleExactValueWithoutTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindSingleValueWithTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate";
        String queryFieldValue = "Aspartate;";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-word
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactValue(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindComplexFieldUsingExactValue(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "(+)-car-3-ene synthase";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindComplexFieldUsingPartialValue(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "(+)-car-3-ene synthase";
        String queryFieldValue = "3-ene synthase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactValueWithTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactValueWithoutTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate aminotransferase";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canBlahBlee(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "aminotransferase Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueWithTerminatingSemiColon(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate aminotransferase;";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindMultiWordValueGivenOneWord(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindMultiWordValueGivenOneWordWhenTheresATerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-word terms
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactValueContainingComma(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "Aspartate aminotransferase, mitochondrial";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueContainingCommaWithoutTerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "L-topaquinone(1-) residue [79027]";
        String queryFieldValue = "L-topaquinone(1-)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindMultiWordValueContainingCommaGivenOneWordWhenTheresATerminatingSemiColon(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "Aspartate, aminotransferase;";
        String queryFieldValue = "Aspartate";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // multi-words with slashes
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindMultiWordValueWithSlash(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue =
                "Influenza A virus (strain A/Goose/Guangdong/1/1996 H5N1 genotype Gs/Gd)";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    // words containing special chars (just using '_' in tests, but applies to all non-word chars)
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueWithUnderScore(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void caseInsensitiveFindValueWithUnderScores(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), fieldValue.toLowerCase());

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canUseFirstPartOfValueToFindValueWithUnderScores(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "VARV");

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canUseMiddlePartOfValueToFindValueWithUnderScores(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "vel4");

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canUseMiddlePartsOfValueToFindValueWithUnderScores(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "VARV_IND64_vel4_019";
        String query = fieldQuery(field.getQueryField(), "IND64_vel4");

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueThatIsOnlyANumber(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "62";
        String query = fieldQuery(field.getQueryField(), fieldValue);
        System.out.println(query);
        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValuesContainingSpecialChars(FullCISplitNumericAnalysisSearchIT.FieldType field) {
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

            new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                    .withAccession(accession)
                    .withFieldValue(fieldValue)
                    .usingQuery(query)
                    .canBeFound(field);

            searchEngine.removeEntry(accession);
        }
    }

    @Disabled
    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueViaAlternativeSpellingFromSynonymList(
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
        // ensure synonyms in are used:
        //
        // uniprot-data-services/data-service-deployments/src/main/distros/solr-conf/homes/uniprot-cores/uniprot/conf/synonyms.txt
        String accession = newAccession();
        String indexFieldValue = "hemoglobin tumor";
        String queryFieldValue = "haemoglobin tumour";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindValueViaWithoutPossessive(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String indexFieldValue = "this was bill's";
        String queryFieldValue = "this was bill";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactPhraseWithDash(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();
        String fieldValue = "LAGE-1, a new gene with tumor specificity.";
        String query = fieldPhraseQuery(field.getQueryField(), fieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(fieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactPhraseWithDash2(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String queryFieldValue = "Cys-tRNA(Pro)";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
                .withAccession(accession)
                .withFieldValue(indexFieldValue)
                .usingQuery(query)
                .canBeFound(field);
    }

    @ParameterizedTest
    @EnumSource(FullCISplitNumericAnalysisSearchIT.FieldType.class)
    void canFindExactPhraseWithDash3(FullCISplitNumericAnalysisSearchIT.FieldType field) {
        String accession = newAccession();

        String indexFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String queryFieldValue = "Cys-tRNA(Pro)/cys-tRNA(Cys) deacylae";
        String query = fieldQuery(field.getQueryField(), queryFieldValue);

        new FullCISplitNumericAnalysisSearchIT.EntryCheck()
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

    private void index(
            String accession,
            String fieldValue,
            FullCISplitNumericAnalysisSearchIT.FieldType field) {
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

    class EntryCheck {
        String accession;
        String fieldValue;
        String query;

        FullCISplitNumericAnalysisSearchIT.EntryCheck withAccession(String accession) {
            this.accession = accession;
            return this;
        }

        FullCISplitNumericAnalysisSearchIT.EntryCheck withFieldValue(String fieldValue) {
            this.fieldValue = fieldValue;
            return this;
        }

        FullCISplitNumericAnalysisSearchIT.EntryCheck usingQuery(String query) {
            this.query = query;
            return this;
        }

        void canBeFound(FullCISplitNumericAnalysisSearchIT.FieldType field) {
            index(accession, fieldValue, field);
            QueryResponse response = searchEngine.getQueryResponse(query);

            List<String> retrievedAccessions = searchEngine.getIdentifiers(response);
            assertThat(retrievedAccessions, contains(accession));
        }
    }

    enum FieldType {
        id_default(TypeFunctions.STRING_LIST_FUNCTION, "id_default");

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
        }
    }
}
