package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;
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
        Tuple2<String, Tuple2<String, ChebiEntry>> tuple =
                new Tuple2<>("Id1", new Tuple2<>("Id1", chebi));

        ChebiToSuggestDocument chebiToSuggestDocument = new ChebiToSuggestDocument("testName1");
        Iterator<Tuple2<String, SuggestDocument>> result = chebiToSuggestDocument.call(tuple);
        assertNotNull(result);

        ArrayList<Tuple2<String, SuggestDocument>> docs = new ArrayList<>();
        result.forEachRemaining(docs::add);
        assertEquals(1, docs.size());
        SuggestDocument doc = docs.get(0)._2;
        assertEquals("testName1", doc.dictionary);
        assertEquals("CHEBI:Id1", doc.id);
        assertEquals("Name1", doc.value);
        assertTrue(doc.altValues.isEmpty());
        assertEquals("medium", doc.importance);
    }

    @Test
    void callWithAlternativeValue() throws Exception {
        ChebiEntry chebi =
                new ChebiEntryBuilder()
                        .id("Id2")
                        .name("Name2")
                        .inchiKey("inch2")
                        .synonymsAdd("synonym2")
                        .build();
        Tuple2<String, Tuple2<String, ChebiEntry>> tuple =
                new Tuple2<>("Id1", new Tuple2<>("Id2", chebi));

        ChebiToSuggestDocument chebiToSuggestDocument = new ChebiToSuggestDocument("testName2");
        Iterator<Tuple2<String, SuggestDocument>> result = chebiToSuggestDocument.call(tuple);
        assertNotNull(result);

        ArrayList<Tuple2<String, SuggestDocument>> docs = new ArrayList<>();
        result.forEachRemaining(docs::add);
        assertEquals(1, docs.size());
        SuggestDocument doc = docs.get(0)._2;
        assertEquals("testName2", doc.dictionary);
        assertEquals("CHEBI:Id2", doc.id);
        assertEquals("Name2", doc.value);
        assertNotNull(doc.altValues);
        assertEquals(2, doc.altValues.size());
        assertTrue(doc.altValues.contains("inch2"));
        assertTrue(doc.altValues.contains("synonym2"));
        assertEquals("medium", doc.importance);
    }

    @Test
    void callWithAlternativeAndRelatedIds() throws Exception {
        ChebiEntry related1 =
                new ChebiEntryBuilder()
                        .id("relatedId1")
                        .name("relatedName1")
                        .inchiKey("relatedInch1")
                        .synonymsAdd("relatedSynonym1")
                        .build();

        ChebiEntry related2 =
                new ChebiEntryBuilder()
                        .id("relatedId2")
                        .name("relatedName2")
                        .inchiKey("relatedInch2")
                        .synonymsAdd("relatedSynonym2")
                        .build();

        ChebiEntry chebi =
                new ChebiEntryBuilder()
                        .id("Id2")
                        .name("Name2")
                        .inchiKey("inch2")
                        .synonymsAdd("synonym2")
                        .relatedIdsAdd(related1)
                        .relatedIdsAdd(related2)
                        .build();

        Tuple2<String, Tuple2<String, ChebiEntry>> tuple =
                new Tuple2<>("Id1", new Tuple2<>("Id2", chebi));

        ChebiToSuggestDocument chebiToSuggestDocument = new ChebiToSuggestDocument("testName3");
        Iterator<Tuple2<String, SuggestDocument>> result = chebiToSuggestDocument.call(tuple);
        assertNotNull(result);

        ArrayList<Tuple2<String, SuggestDocument>> docs = new ArrayList<>();
        result.forEachRemaining(docs::add);
        assertEquals(3, docs.size());

        SuggestDocument doc = docs.get(0)._2;
        assertEquals("testName3", doc.dictionary);
        assertEquals("CHEBI:Id2", doc.id);
        assertEquals("Name2", doc.value);
        assertNotNull(doc.altValues);
        assertEquals(2, doc.altValues.size());
        assertTrue(doc.altValues.contains("inch2"));
        assertTrue(doc.altValues.contains("synonym2"));
        assertEquals("medium", doc.importance);

        doc = docs.get(1)._2;
        assertEquals("testName3", doc.dictionary);
        assertEquals("CHEBI:relatedId1", doc.id);
        assertEquals("relatedName1", doc.value);
        assertNotNull(doc.altValues);
        assertEquals(2, doc.altValues.size());
        assertTrue(doc.altValues.contains("relatedInch1"));
        assertTrue(doc.altValues.contains("relatedSynonym1"));
        assertEquals("medium", doc.importance);

        doc = docs.get(2)._2;
        assertEquals("testName3", doc.dictionary);
        assertEquals("CHEBI:relatedId2", doc.id);
        assertEquals("relatedName2", doc.value);
        assertNotNull(doc.altValues);
        assertEquals(2, doc.altValues.size());
        assertTrue(doc.altValues.contains("relatedInch2"));
        assertTrue(doc.altValues.contains("relatedSynonym2"));
        assertEquals("medium", doc.importance);
    }
}
