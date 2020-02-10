package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.cv.subcell.impl.SubcellularLocationEntryImpl;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class SubcellularLocationToSuggestDocumentTest {

    @Test
    void testSubcellularLocationToSuggestDocument() throws Exception {
        SubcellularLocationEntryImpl entry = new SubcellularLocationEntryImpl();
        entry.setId("slId");
        entry.setAccession("slAcc");
        SubcellularLocationToSuggestDocument mapper = new SubcellularLocationToSuggestDocument();

        SuggestDocument result = mapper.call(entry);
        assertNotNull(result);

        assertEquals("SUBCELL", result.dictionary);
        assertEquals("slAcc", result.id);
        assertEquals("slId", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }
}
