package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.keyword.KeywordEntry;
import org.uniprot.core.cv.keyword.impl.KeywordEntryImpl;
import org.uniprot.core.cv.keyword.impl.KeywordImpl;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class KeywordToSuggestDocumentTest {

    @Test
    void testKeywordToSuggestDocumentWithoutCategory() throws Exception {
        KeywordEntryImpl keywordEntry = new KeywordEntryImpl();
        keywordEntry.setKeyword(new KeywordImpl("kwId","kwAcc"));

        KeywordToSuggestDocument mapper = new KeywordToSuggestDocument();
        Iterable<SuggestDocument> results = mapper.call(new Tuple2<>("kwIdId",keywordEntry));

        assertNotNull(results);
        List<SuggestDocument> resultList = new ArrayList<>();
        results.forEach(resultList::add);
        assertEquals(1, resultList.size());

        SuggestDocument result = resultList.get(0);
        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwAcc", result.id);
        assertEquals("kwId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testKeywordToSuggestDocumentWithCategory() throws Exception {
        KeywordEntryImpl keywordEntry = new KeywordEntryImpl();
        keywordEntry.setKeyword(new KeywordImpl("kwId","kwAcc"));
        keywordEntry.setCategory(new KeywordImpl("kwCatId","kwCatAcc"));

        KeywordToSuggestDocument mapper = new KeywordToSuggestDocument();
        Iterable<SuggestDocument> results = mapper.call(new Tuple2<>("kwIdId",keywordEntry));

        assertNotNull(results);
        List<SuggestDocument> resultList = new ArrayList<>();
        results.forEach(resultList::add);
        assertEquals(2, resultList.size());

        SuggestDocument result = resultList.get(0);
        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwAcc", result.id);
        assertEquals("kwId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);

        result = resultList.get(1);
        assertEquals("KEYWORD", result.dictionary);
        assertEquals("kwCatAcc", result.id);
        assertEquals("kwCatId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }
}
