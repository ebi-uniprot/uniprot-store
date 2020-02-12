package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.core.cv.ec.ECEntryImpl;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class ECToSuggestDocumentTest {

    @Test
    void testECToSuggestDocument() throws Exception {
        ECToSuggestDocument mapper = new ECToSuggestDocument();
        ECEntry ec = new ECEntryImpl("ecId", "ecLabel");
        SuggestDocument result = mapper.call(new Tuple2<>("ecId", ec));

        assertNotNull(result);

        assertEquals("ECEntry", result.dictionary);
        assertEquals("ecId", result.id);
        assertEquals("ecLabel", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }
}
