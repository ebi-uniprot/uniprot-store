package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.impl.KeywordEntryImpl;
import org.uniprot.core.cv.keyword.impl.KeywordImpl;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class KeywordToSuggestDocumentTest {

    @Test
    void testKeywordToSuggestDocumentWithoutCategory() throws Exception {
        KeywordEntryImpl keywordEntry = new KeywordEntryImpl();
        keywordEntry.setKeyword(new KeywordImpl("kwId", "kwAcc"));

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
        KeywordEntryImpl keywordEntry = new KeywordEntryImpl();
        keywordEntry.setKeyword(new KeywordImpl("kwId", "kwAcc"));
        keywordEntry.setCategory(new KeywordImpl("kwCatId", "kwCatAcc"));

        KeywordToSuggestDocument mapper = new KeywordToSuggestDocument();
        SuggestDocument result = mapper.call(keywordEntry);

        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwAcc", result.id);
        assertEquals("kwId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }
}
