package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.go.GeneOntologyEntry;
import org.uniprot.core.cv.go.impl.GeneOntologyEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class GOToSuggestDocumentTest {

    @Test
    void testGOToSuggestDocumentWithoutAncestors() throws Exception {
        GeneOntologyEntry term =
                new GeneOntologyEntryBuilder().id("GO:goId").name("goName").build();

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
        assertTrue(result.altValues.contains("GO:goId"));
        assertEquals("medium", result.importance);
    }

    @Test
    void testGOToSuggestDocumentWithAncestors() throws Exception {
        GeneOntologyEntry term =
                new GeneOntologyEntryBuilder()
                        .id("GO:goId")
                        .name("goName")
                        .ancestorsAdd(
                                new GeneOntologyEntryBuilder()
                                        .id("GO:goAncestor1")
                                        .name("goAncestorName1")
                                        .build())
                        .ancestorsAdd(
                                new GeneOntologyEntryBuilder()
                                        .id("GO:goAncestor2")
                                        .name("goAncestorName2")
                                        .build())
                        .build();

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
        assertTrue(result.altValues.contains("GO:goId"));
        assertEquals("medium", result.importance);

        result = resultList.get(1);
        assertEquals("GO", result.dictionary);
        assertEquals("goAncestor2", result.id);
        assertEquals("goAncestorName2", result.value);
        assertTrue(result.altValues.contains("GO:goAncestor2"));
        assertEquals("medium", result.importance);
    }
}
