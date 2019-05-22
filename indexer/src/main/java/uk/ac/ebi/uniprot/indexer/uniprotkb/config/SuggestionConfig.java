package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.cv.xdb.UniProtXDbTypes;
import uk.ac.ebi.uniprot.domain.uniprot.comment.CommentType;
import uk.ac.ebi.uniprot.domain.uniprot.feature.FeatureCategory;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDictionary;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created 16/05/19
 *
 * @author Edd
 */
@Configuration
public class SuggestionConfig {
    private static final String DEFAULT_TAXON_SYNONYMS_FILE = "default-taxon-synonyms.txt";
    private static final String COMMENT_LINE_PREFIX = "#";

    @Bean
    public Map<String, SuggestDocument> suggestDocuments() {
        Map<String, SuggestDocument> suggestionMap = new HashMap<>();
        loadDefaultTaxonSynonymSuggestions(suggestionMap);
        loadDefaultMainSuggestions(suggestionMap);
        return suggestionMap;
    }

    private void loadDefaultMainSuggestions(Map<String, SuggestDocument> suggestionMap) {
        Consumer<SuggestDocument> suggestionMapUpdater = suggestion -> suggestionMap.put(suggestion.value, suggestion);

        enumToSuggestions(new FeatureCategoryToSuggestion(), suggestionMapUpdater);
        enumToSuggestions(new CommentTypeToSuggestion(), suggestionMapUpdater);
        databaseSuggestions(suggestionMapUpdater);
    }

    private void loadDefaultTaxonSynonymSuggestions(Map<String, SuggestDocument> suggestionMap) {
        InputStream inputStream = SuggestionConfig.class.getClassLoader()
                .getResourceAsStream(DEFAULT_TAXON_SYNONYMS_FILE);
        if (inputStream != null) {
            try (Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines()) {
                lines.map(this::createDefaultTaxonomySuggestion)
                        .filter(Objects::nonNull)
                        .forEach(suggestion -> suggestionMap.put(SuggestDictionary.TAXONOMY.name() + ":" + suggestion.id, suggestion));
            }
        }
    }

    private SuggestDocument createDefaultTaxonomySuggestion(String csvLine) {
        String[] lineParts = csvLine.split("\t");
        if (!csvLine.startsWith(COMMENT_LINE_PREFIX) && lineParts.length == 4) {
            return SuggestDocument.builder()
                    .value(lineParts[0])
                    .altValues(Stream.of(lineParts[2].split(",")).collect(Collectors.toList()))
                    .id(lineParts[1])
                    .importance(lineParts[3])
                    .dictionary(SuggestDictionary.TAXONOMY.name())
                    .build();
        } else {
            return null;
        }
    }

    private static void databaseSuggestions(Consumer<SuggestDocument> suggestionMapUpdater) {
        UniProtXDbTypes.INSTANCE.getAllDBXRefTypes().stream()
                .map(type -> {
                    String name = removeTerminalSemiColon(type.getDisplayName());
                    return SuggestDocument.builder()
                            .value("Database: " + name)
                            .dictionary(SuggestDictionary.MAIN.name())
                            .build();
                })
                .forEach(suggestionMapUpdater);
    }

    private static String removeTerminalSemiColon(String displayName) {
        int charIndex = displayName.indexOf(';');
        if (charIndex < 0) {
            return displayName;
        } else {
            return displayName.substring(0, charIndex);
        }
    }

    private <T extends Enum<T>> void enumToSuggestions(EnumSuggestionFunction<T> typeToSuggestion, Consumer<SuggestDocument> handler) {
        Stream.of(typeToSuggestion.getEnumType().getEnumConstants())
                .map(typeToSuggestion)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(handler);
    }

    interface EnumSuggestionFunction<T> extends Function<T, Optional<SuggestDocument>> {
        Class<T> getEnumType();
    }

    static class FeatureCategoryToSuggestion implements EnumSuggestionFunction<FeatureCategory> {
        @Override
        public Optional<SuggestDocument> apply(FeatureCategory value) {
            String name = value.name();
            return Optional.of(SuggestDocument.builder()
                                       .value("Feature Category: " + name)
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
            return value == CommentType.UNKNOWN ?
                    Optional.empty() :
                    Optional.of(SuggestDocument.builder()
                                        .value("Comment type: " + name)
                                        .dictionary(SuggestDictionary.MAIN.name())
                                        .build());
        }

        @Override
        public Class<CommentType> getEnumType() {
            return CommentType.class;
        }
    }
}
