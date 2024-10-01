package org.uniprot.store.spark.indexer.suggest.mapper;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import static org.junit.jupiter.api.Assertions.*;

class SuggestDocumentAggregatorTest {

    @Test
    void aggregateSuggestDocumentOneOnly() throws Exception {
        SuggestDocumentAggregator mapper = new SuggestDocumentAggregator();
        SuggestDocument docValue = new SuggestDocument();
        SuggestDocument result = mapper.call(docValue, null);
        assertNotNull(result);
        assertEquals(docValue, result);
    }

    @Test
    void aggregateSuggestDocumentTwoOnly() throws Exception {
        SuggestDocumentAggregator mapper = new SuggestDocumentAggregator();
        SuggestDocument docValue = new SuggestDocument();
        SuggestDocument result = mapper.call(null, docValue);
        assertNotNull(result);
        assertEquals(docValue, result);
    }

    @Test
    void aggregateMerge() throws Exception {
        SuggestDocumentAggregator mapper = new SuggestDocumentAggregator();
        SuggestDocument docValue1 = new SuggestDocument();
        docValue1.id = "value1";
        SuggestDocument docValue2 = new SuggestDocument();
        docValue2.id = "value2";
        SuggestDocument result = mapper.call(docValue1, docValue2);
        assertNotNull(result);
        assertEquals(docValue1, result);
    }
}