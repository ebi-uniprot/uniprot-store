package org.uniprot.store.indexer.uniprotkb.config;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uniprot.core.uniprot.comment.CommentType;
import org.uniprot.core.uniprot.feature.FeatureCategory;
import org.uniprot.cv.xdb.UniProtXDbTypes;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * Created 16/05/19
 *
 * @author Edd
 */
@Configuration
public class SuggestionConfig {
    public static final String DEFAULT_TAXON_SYNONYMS_FILE = "default-taxon-synonyms.txt";
    public static final String DEFAULT_HOST_SYNONYMS_FILE = "default-host-synonyms.txt";
    private static final String COMMENT_LINE_PREFIX = "#";
    static final String DATABASE_PREFIX = "Database: ";
    static final String FEATURE_CATEGORY_PREFIX = "Feature Category: ";
    static final String COMMENT_TYPE_PREFIX = "Comment type: ";

    @Bean
    public Map<String, SuggestDocument> suggestDocuments() {
        Map<String, SuggestDocument> suggestionMap = new ConcurrentHashMap<>();

        loadDefaultMainSuggestions()
                .forEach(suggestion -> suggestionMap.put(suggestion.value, suggestion));

        loadDefaultTaxonSynonymSuggestions(SuggestDictionary.TAXONOMY, DEFAULT_TAXON_SYNONYMS_FILE)
                .forEach(
                        suggestion ->
                                suggestionMap.put(
                                        SuggestDictionary.TAXONOMY.name() + ":" + suggestion.id,
                                        suggestion));

        loadDefaultTaxonSynonymSuggestions(SuggestDictionary.ORGANISM, DEFAULT_TAXON_SYNONYMS_FILE)
                .forEach(
                        suggestion ->
                                suggestionMap.put(
                                        SuggestDictionary.ORGANISM.name() + ":" + suggestion.id,
                                        suggestion));

        loadDefaultTaxonSynonymSuggestions(SuggestDictionary.HOST, DEFAULT_HOST_SYNONYMS_FILE)
                .forEach(
                        suggestion ->
                                suggestionMap.put(
                                        SuggestDictionary.HOST.name() + ":" + suggestion.id,
                                        suggestion));
        return suggestionMap;
    }

    private List<SuggestDocument> loadDefaultMainSuggestions() {
        List<SuggestDocument> defaultSuggestions = new ArrayList<>();

        defaultSuggestions.addAll(enumToSuggestions(new FeatureCategoryToSuggestion()));
        defaultSuggestions.addAll(enumToSuggestions(new CommentTypeToSuggestion()));
        defaultSuggestions.addAll(databaseSuggestions());

        return defaultSuggestions;
    }

    public List<SuggestDocument> loadDefaultTaxonSynonymSuggestions(
            SuggestDictionary dict, String taxonSynonymFile) {
        List<SuggestDocument> taxonSuggestions = new ArrayList<>();
        InputStream inputStream =
                SuggestionConfig.class.getClassLoader().getResourceAsStream(taxonSynonymFile);
        if (inputStream != null) {
            try (Stream<String> lines =
                    new BufferedReader(new InputStreamReader(inputStream)).lines()) {
                lines.map(val -> createDefaultTaxonomySuggestion(val, dict))
                        .filter(Objects::nonNull)
                        .forEach(taxonSuggestions::add);
            }
        }

        return taxonSuggestions;
    }

    private SuggestDocument createDefaultTaxonomySuggestion(
            String csvLine, SuggestDictionary dict) {
        String[] lineParts = csvLine.split("\t");
        if (!csvLine.startsWith(COMMENT_LINE_PREFIX) && lineParts.length == 3) {
            return SuggestDocument.builder()
                    .altValues(Stream.of(lineParts[1].split(",")).collect(Collectors.toList()))
                    .id(lineParts[0].trim())
                    .importance(lineParts[2].trim())
                    .dictionary(dict.name())
                    .build();
        } else {
            return null;
        }
    }

    public static List<SuggestDocument> databaseSuggestions() {
        return UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                .filter(val -> !val.isImplicit())
                .map(
                        type -> {
                            String name = removeTerminalSemiColon(type.getDisplayName());
                            return SuggestDocument.builder()
                                    .value(DATABASE_PREFIX + name)
                                    .dictionary(SuggestDictionary.MAIN.name())
                                    .build();
                        })
                .collect(Collectors.toList());
    }

    static String removeTerminalSemiColon(String displayName) {
        int charIndex = displayName.indexOf(';');
        if (charIndex < 0) {
            return displayName;
        } else {
            return displayName.substring(0, charIndex);
        }
    }

    private <T extends Enum<T>> List<SuggestDocument> enumToSuggestions(
            EnumSuggestionFunction<T> typeToSuggestion) {
        return Stream.of(typeToSuggestion.getEnumType().getEnumConstants())
                .map(typeToSuggestion)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    interface EnumSuggestionFunction<T> extends Function<T, Optional<SuggestDocument>> {
        Class<T> getEnumType();
    }

    static class FeatureCategoryToSuggestion implements EnumSuggestionFunction<FeatureCategory> {
        @Override
        public Optional<SuggestDocument> apply(FeatureCategory value) {
            String name = value.name();
            return Optional.of(
                    SuggestDocument.builder()
                            .value(FEATURE_CATEGORY_PREFIX + name)
                            .dictionary(SuggestDictionary.MAIN.name())
                            .build());
        }

        @Override
        public Class<FeatureCategory> getEnumType() {
            return FeatureCategory.class;
        }
    }

    static class CommentTypeToSuggestion implements EnumSuggestionFunction<CommentType> {
        @Override
        public Optional<SuggestDocument> apply(CommentType value) {
            String name = value.toXmlDisplayName();
            return value == CommentType.UNKNOWN
                    ? Optional.empty()
                    : Optional.of(
                            SuggestDocument.builder()
                                    .value(COMMENT_TYPE_PREFIX + name)
                                    .dictionary(SuggestDictionary.MAIN.name())
                                    .build());
        }

        @Override
        public Class<CommentType> getEnumType() {
            return CommentType.class;
        }
    }
}
