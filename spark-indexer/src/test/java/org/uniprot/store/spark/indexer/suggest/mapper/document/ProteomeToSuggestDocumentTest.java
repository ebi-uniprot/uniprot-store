package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

/**
 * @author sahmad
 * @created 24/08/2020
 */
class ProteomeToSuggestDocumentTest {
    @Test
    void testCreateDocument() throws Exception {
        String pId = "UP000008595";
        String pName = "Uukuniemi virus (strain S23) (Uuk)";
        ProteomeEntryBuilder builder = new ProteomeEntryBuilder();
        builder.proteomeId(pId).description(pName);
        ProteomeEntry entry = builder.build();
        ProteomeToSuggestDocument documentConverter = new ProteomeToSuggestDocument();
        SuggestDocument document = documentConverter.call(entry);
        assertNotNull(document);
        assertEquals(pId, document.id);
        assertEquals(pName, document.value);
        assertEquals(1, document.altValues.size());
        assertEquals(pId, document.altValues.get(0));
    }
}
