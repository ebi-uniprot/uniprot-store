package org.uniprot.store.spark.indexer.suggest.mapper.document;

import org.apache.spark.api.java.function.Function;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * This class converts a Keyword entry to a SuggestDocument
 *
 * @author lgonzales
 * @since 2020-01-16
 */
public class KeywordToSuggestDocument implements Function<KeywordEntry, SuggestDocument> {
    private static final long serialVersionUID = -4656249467275467458L;

    @Override
    public SuggestDocument call(KeywordEntry keyword) throws Exception {
        return SuggestDocument.builder()
                .id(keyword.getKeyword().getAccession())
                .value(keyword.getKeyword().getId())
                .dictionary(SuggestDictionary.KEYWORD.name())
                .build();
    }
}
