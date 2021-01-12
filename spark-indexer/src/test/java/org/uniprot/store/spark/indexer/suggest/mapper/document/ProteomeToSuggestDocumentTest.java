package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 24/08/2020
 */
class ProteomeToSuggestDocumentTest {
    @Test
    void testCreateDocument() throws Exception {
        String pId = "UP000008595";
        String pName = "Uukuniemi virus (strain S23) (Uuk)";
        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
        builder.taxonId(11111).scientificName(pName);
        TaxonomyEntry entry = builder.build();
        Tuple2<String, TaxonomyEntry> tuple = new Tuple2<>(pId, entry);
        ProteomeToSuggestDocument documentConverter = new ProteomeToSuggestDocument();
        SuggestDocument document = documentConverter.call(tuple);
        assertNotNull(document);
        assertEquals(pId, document.id);
        assertEquals(pName, document.value);
        assertEquals(1, document.altValues.size());
        assertEquals(pId, document.altValues.get(0));
    }
}
