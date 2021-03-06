package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.KeywordId;
import org.uniprot.core.cv.keyword.impl.KeywordEntryBuilder;
import org.uniprot.core.cv.keyword.impl.KeywordIdBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class KeywordToSuggestDocumentTest {

    @Test
    void testKeywordToSuggestDocumentWithoutCategory() throws Exception {
        KeywordEntry keywordEntry = new KeywordEntryBuilder().keyword(kw("kwId", "kwAcc")).build();

        KeywordToSuggestDocument mapper = new KeywordToSuggestDocument();
        SuggestDocument result = mapper.call(keywordEntry);

        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwAcc", result.id);
        assertEquals("kwId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testKeywordToSuggestDocumentWithCategory() throws Exception {
        KeywordEntry keywordEntry =
                new KeywordEntryBuilder()
                        .keyword(kw("kwId", "kwAcc"))
                        .category(kw("kwCatId", "kwCatAcc"))
                        .build();

        KeywordToSuggestDocument mapper = new KeywordToSuggestDocument();
        SuggestDocument result = mapper.call(keywordEntry);

        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwAcc", result.id);
        assertEquals("kwId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    private KeywordId kw(String id, String accession) {
        return new KeywordIdBuilder().name(id).id(accession).build();
    }
}
