package org.uniprot.store.spark.indexer.suggest.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author lgonzales
 * @since 2020-01-22
 */
class TaxonomyHighImportanceReduceTest {

    @Test
    void testDoc1Important() throws Exception {
        TaxonomyHighImportanceReduce reduce = new TaxonomyHighImportanceReduce();
        SuggestDocument doc1 = getImportantDoc();
        SuggestDocument doc2 = getNormalDoc();

        SuggestDocument result = reduce.call(doc1, doc2);
        validateResult(result);
    }

    @Test
    void testDoc2Important() throws Exception {
        TaxonomyHighImportanceReduce reduce = new TaxonomyHighImportanceReduce();
        SuggestDocument doc1 = getNormalDoc();
        SuggestDocument doc2 = getImportantDoc();

        SuggestDocument result = reduce.call(doc2, doc1);
        validateResult(result);
    }

    @Test
    void testBothNormalImportant() throws Exception {
        TaxonomyHighImportanceReduce reduce = new TaxonomyHighImportanceReduce();
        SuggestDocument doc1 = getNormalDoc();
        SuggestDocument doc2 = getNormalDoc();

        SuggestDocument result = reduce.call(doc2, doc1);
        assertNotNull(result);
        assertEquals("medium", result.importance);
        assertEquals("normal value", result.value);
        assertEquals(1, result.altValues.size());
        assertTrue(result.altValues.contains("mormal alt value"));
    }

    private SuggestDocument getNormalDoc() {
        return SuggestDocument.builder()
                .id("111")
                .value("normal value")
                .altValue("mormal alt value")
                .build();
    }

    private void validateResult(SuggestDocument result) {
        assertNotNull(result);
        assertEquals("high", result.importance);
        assertEquals("normal value", result.value);
        assertTrue(result.altValues.contains("mormal alt value"));
        assertTrue(result.altValues.contains("high important alt value"));
    }

    private SuggestDocument getImportantDoc() {
        return SuggestDocument.builder()
                .id("111")
                .altValue("high important alt value")
                .importance("high")
                .build();
    }
}
