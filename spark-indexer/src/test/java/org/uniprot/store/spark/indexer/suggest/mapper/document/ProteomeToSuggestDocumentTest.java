package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
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
        TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
        builder.taxonId(11111).scientificName(pName);
        TaxonomyLineage entry = builder.build();
        var tuple = new Tuple2<>(pId, List.of(entry));
        ProteomeToSuggestDocument documentConverter = new ProteomeToSuggestDocument();
        SuggestDocument document = documentConverter.call(tuple);
        assertNotNull(document);
        assertEquals(pId, document.id);
        assertEquals(pName, document.value);
        assertEquals(1, document.altValues.size());
        assertEquals(pId, document.altValues.get(0));
    }
}
