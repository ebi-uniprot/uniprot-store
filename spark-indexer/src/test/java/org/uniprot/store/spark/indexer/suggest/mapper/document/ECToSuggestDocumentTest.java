package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.store.search.field.SuggestField.Importance.low;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.core.cv.ec.impl.ECEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class ECToSuggestDocumentTest {
    private final ECToSuggestDocument mapper = new ECToSuggestDocument();

    @Test
    void testECToSuggestDocument() throws Exception {
        ECEntry ec = new ECEntryBuilder().id("ecId").label("ecLabel").build();
        SuggestDocument result = mapper.call(new Tuple2<>("ecId", ec));

        assertNotNull(result);

        assertEquals("EC", result.dictionary);
        assertEquals("ecId", result.id);
        assertEquals("ecLabel", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void defaultImportanceIsMedium() throws Exception {
        ECEntry ec = new ECEntryBuilder().id("ecId").label("ecLabel").build();
        SuggestDocument result = mapper.call(new Tuple2<>("ecId", ec));
        assertEquals(SuggestDocument.DEFAULT_IMPORTANCE, result.importance);
    }

    @Test
    void forTransferredEntryImportanceWillBeLow() throws Exception {
        ECEntry ec = new ECEntryBuilder().id("ecId").label("Transferred entry: 1.1.1.239").build();
        SuggestDocument result = mapper.call(new Tuple2<>("ecId", ec));
        assertEquals(low.name(), result.importance);
    }
}
