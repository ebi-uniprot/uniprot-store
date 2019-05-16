package uk.ac.ebi.uniprot.indexer.uniprotkb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.uniprot.indexer.suggest.reader.TaxonomySuggestionItemReader;
import uk.ac.ebi.uniprot.search.document.suggest.SuggestDocument;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
        // TODO: 15/05/19 make this an offheap in memory set, and populate it with taxonomy synonyms?
        Map<String, SuggestDocument> suggestionMap = new HashMap<>();
        loadDefaultSynonyms(suggestionMap);
        return suggestionMap;
    }

    private Map<String, SuggestDocument> loadDefaultSynonyms(Map<String, SuggestDocument> suggestionMap) {
        InputStream inputStream = TaxonomySuggestionItemReader.class.getClassLoader()
                .getResourceAsStream(DEFAULT_TAXON_SYNONYMS_FILE);
        if (inputStream != null) {
            try (Stream<String> lines = new BufferedReader(new InputStreamReader(inputStream)).lines()) {
                lines.map(this::createDefaultSuggestion)
                        .filter(Objects::nonNull)
                        .forEach(suggestion -> suggestionMap.put(suggestion.id, suggestion));
            }
        }
        return suggestionMap;
    }

    private SuggestDocument createDefaultSuggestion(String csvLine) {
        String[] lineParts = csvLine.split("\t");
        if (!csvLine.startsWith(COMMENT_LINE_PREFIX) && lineParts.length == 3) {
            return SuggestDocument.builder()
                    .value(lineParts[0])
                    .altValue(Stream.of(lineParts[2].split(",")).collect(Collectors.toList()))
                    .id(lineParts[1])
                    .build();
        } else {
            return null;
        }
    }
}
