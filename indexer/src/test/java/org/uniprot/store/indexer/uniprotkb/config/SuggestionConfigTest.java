package org.uniprot.store.indexer.uniprotkb.config;

// import org.jetbrains.annotations.NotNull;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.uniprot.store.indexer.uniprotkb.config.SuggestionConfig.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.validation.constraints.NotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.xdb.UniProtDatabaseDetail;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.feature.FeatureCategory;
import org.uniprot.cv.xdb.UniProtDatabaseTypes;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * Created 25/05/19
 *
 * @author Edd
 */
class SuggestionConfigTest {

    private static Map<String, SuggestDocument> suggestionMap;

    @BeforeAll
    static void loadDefaultSuggestions() {
        suggestionMap = new SuggestionConfig().suggestDocuments();
    }

    @Test
    void allDefaultTaxonsLoaded() {
        List<SuggestDocument> taxonSuggestions =
                extractSuggestDocuments(SuggestDictionary.TAXONOMY.name());
        assertThat(taxonSuggestions, hasSize(35));

        List<SuggestDocument> xenopusTropicalisList =
                taxonSuggestions.stream()
                        .filter(suggestion -> suggestion.id.equals("8364"))
                        .collect(Collectors.toList());
        assertThat(xenopusTropicalisList, hasSize(1));
        SuggestDocument xenopusTropicalis = xenopusTropicalisList.get(0);
        assertThat(
                xenopusTropicalis.altValues,
                contains(
                        "Western clawed frog",
                        "Tropical clawed frog",
                        "X. tropicalis",
                        "Xenopus laevis tropicalis",
                        "Silurana tropicalis"));
        assertThat(xenopusTropicalis.id, is("8364"));
        assertThat(xenopusTropicalis.importance, is("high"));
    }

    @Test
    void allDefaultDatabasesLoaded() {
        List<SuggestDocument> suggestions = extractSuggestDocuments("Database:");
        List<UniProtDatabaseDetail> dbxRefTypes =
                UniProtDatabaseTypes.INSTANCE.getAllDbTypes().stream()
                        .filter(val -> !val.isImplicit())
                        .collect(Collectors.toList());
        assertThat(dbxRefTypes, hasSize(greaterThan(0)));
        assertThat(suggestions, hasSize(dbxRefTypes.size()));

        List<SuggestDocument> firstDbXrefTypeList =
                suggestions.stream()
                        .filter(
                                suggestion ->
                                        suggestion.value.equals(
                                                DATABASE_PREFIX
                                                        + dbxRefTypes.get(0).getDisplayName()))
                        .collect(Collectors.toList());
        assertThat(
                firstDbXrefTypeList.get(0).value,
                is(DATABASE_PREFIX + dbxRefTypes.get(0).getDisplayName()));
    }

    @Test
    void allDefaultFeatureCategoriesLoaded() {
        List<SuggestDocument> suggestions = extractSuggestDocuments(FEATURE_CATEGORY_PREFIX);

        List<FeatureCategory> featureCategories = asList(FeatureCategory.values());
        assertThat(featureCategories, hasSize(greaterThan(0)));
        assertThat(suggestions, hasSize(featureCategories.size()));

        List<SuggestDocument> specificCategoryList =
                suggestions.stream()
                        .filter(
                                suggestion ->
                                        suggestion.value.equals(
                                                FEATURE_CATEGORY_PREFIX
                                                        + featureCategories.get(0).name()))
                        .collect(Collectors.toList());
        assertThat(
                specificCategoryList.get(0).value,
                is(FEATURE_CATEGORY_PREFIX + featureCategories.get(0).name()));
    }

    @Test
    void allDefaultCommentTypesLoaded() {
        List<SuggestDocument> suggestions = extractSuggestDocuments(COMMENT_TYPE_PREFIX);

        List<CommentType> commentTypes =
                Arrays.stream(CommentType.values())
                        .filter(type -> type != CommentType.UNKNOWN)
                        .collect(Collectors.toList());
        assertThat(commentTypes, hasSize(greaterThan(0)));
        assertThat(suggestions, hasSize(commentTypes.size()));

        List<SuggestDocument> specificTypeList =
                suggestions.stream()
                        .filter(
                                suggestion ->
                                        suggestion.value.equals(
                                                COMMENT_TYPE_PREFIX
                                                        + commentTypes.get(0).toXmlDisplayName()))
                        .collect(Collectors.toList());
        assertThat(
                specificTypeList.get(0).value,
                is(COMMENT_TYPE_PREFIX + commentTypes.get(0).toXmlDisplayName()));
    }

    @Test
    void removesTerminalSemiColonWhenPresent() {
        String value = "string ending in";
        String terminalSemiColonValue = value + ";";
        String formattedValue = removeTerminalSemiColon(terminalSemiColonValue);
        assertThat(formattedValue, is(value));
    }

    @Test
    void removingTerminalSemiColonWhenNotPresentDoesNothing() {
        String value = "string ending in full-stop.";
        String formattedValue = removeTerminalSemiColon(value);
        assertThat(formattedValue, is(value));
    }

    @NotNull
    private List<SuggestDocument> extractSuggestDocuments(String keyPrefix) {
        return suggestionMap.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(keyPrefix))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }
}
