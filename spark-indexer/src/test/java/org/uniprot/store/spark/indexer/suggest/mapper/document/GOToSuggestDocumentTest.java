package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.go.relations.GOTerm;
import org.uniprot.store.spark.indexer.go.relations.GOTermImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class GOToSuggestDocumentTest {

    @Test
    void testGOToSuggestDocumentWithoutAncestors() throws Exception {
        GOTerm term = new GOTermImpl("goId", "goName");

        GOToSuggestDocument mapper = new GOToSuggestDocument();
        Iterable<SuggestDocument> results = mapper.call(new Tuple2<>(term, "goIdId"));

        assertNotNull(results);
        List<SuggestDocument> resultList = new ArrayList<>();
        results.forEach(resultList::add);
        assertEquals(1, resultList.size());

        SuggestDocument result = resultList.get(0);
        assertEquals("GO", result.dictionary);
        assertEquals("goId", result.id);
        assertEquals("goName", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testGOToSuggestDocumentWithAncestors() throws Exception {
        Set<GOTerm> ancestors = new HashSet<>();
        ancestors.add(new GOTermImpl("goAncestor1", "goAncestorName1"));
        ancestors.add(new GOTermImpl("goAncestor2", "goAncestorName2"));
        GOTerm term = new GOTermImpl("goId", "goName", ancestors);

        GOToSuggestDocument mapper = new GOToSuggestDocument();
        Iterable<SuggestDocument> results = mapper.call(new Tuple2<>(term, "goIdId"));

        assertNotNull(results);
        List<SuggestDocument> resultList = new ArrayList<>();
        results.forEach(resultList::add);
        assertEquals(3, resultList.size());

        SuggestDocument result = resultList.get(0);
        assertEquals("GO", result.dictionary);
        assertEquals("goId", result.id);
        assertEquals("goName", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);

        result = resultList.get(1);
        assertEquals("GO", result.dictionary);
        assertEquals("goAncestor2", result.id);
        assertEquals("goAncestorName2", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }
}
