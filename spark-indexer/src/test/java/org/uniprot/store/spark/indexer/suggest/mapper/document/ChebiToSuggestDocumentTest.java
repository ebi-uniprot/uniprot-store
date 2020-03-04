package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.builder.ChebiEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class ChebiToSuggestDocumentTest {

    @Test
    void callWithoutAlternativeValue() throws Exception {
        ChebiEntry chebi = new ChebiEntryBuilder().id("Id1").name("Name1").build();
        Tuple2<String, ChebiEntry> tuple = new Tuple2<>("Id1", chebi);

        ChebiToSuggestDocument chebiToSuggestDocument = new ChebiToSuggestDocument("testName1");
        SuggestDocument result = chebiToSuggestDocument.call(tuple);
        assertNotNull(result);

        assertEquals("testName1", result.dictionary);
        assertEquals("Id1", result.id);
        assertEquals("Name1", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void callWithAlternativeValue() throws Exception {
        ChebiEntry chebi =
                new ChebiEntryBuilder().id("Id2").name("Name2").inchiKey("inch2").build();
        Tuple2<String, ChebiEntry> tuple = new Tuple2<>("Id2", chebi);

        ChebiToSuggestDocument chebiToSuggestDocument = new ChebiToSuggestDocument("testName2");
        SuggestDocument result = chebiToSuggestDocument.call(tuple);
        assertNotNull(result);
    }
}
