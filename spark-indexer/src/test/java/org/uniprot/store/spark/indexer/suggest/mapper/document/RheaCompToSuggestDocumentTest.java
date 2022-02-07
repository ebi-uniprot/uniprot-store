package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;
import org.uniprot.store.spark.indexer.rhea.model.RheaComp;

import scala.Tuple2;

class RheaCompToSuggestDocumentTest {

    @Test
    void canMapToSuggestDocument() throws Exception {
        String id = "id-value";
        String name = "name-value";
        RheaCompToSuggestDocument mapper = new RheaCompToSuggestDocument();
        RheaComp comp = RheaComp.builder().id(id).name(name).build();
        Tuple2<String, RheaComp> tuple = new Tuple2<>(comp.getId(), comp);
        SuggestDocument doc = mapper.call(tuple);
        assertNotNull(doc);
        assertEquals(id, doc.id);
        assertEquals(name, doc.value);
        assertEquals(SuggestDictionary.CATALYTIC_ACTIVITY.name(), doc.dictionary);
        assertEquals("medium", doc.importance);
        assertNotNull(doc.altValues);
        assertTrue(doc.altValues.isEmpty());
    }
}
