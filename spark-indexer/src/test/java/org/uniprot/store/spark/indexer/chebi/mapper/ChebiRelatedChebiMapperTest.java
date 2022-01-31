package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

class ChebiRelatedChebiMapperTest {

    @Test
    void canMapRelatedChebiEntry() throws Exception {
        ChebiRelatedChebiMapper mapper = new ChebiRelatedChebiMapper();
        Long relatedId = 2222L;
        ChebiEntry relatedChebiEntry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(relatedId))
                        .name("entryName")
                        .inchiKey("InchValue")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("444").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("555").build())
                        .build();
        Long chebiId = 11111L;
        Tuple2<ChebiEntry, Long> chebiTuple = new Tuple2<>(relatedChebiEntry, chebiId);

        // Tuple2<RelatedId, Tuple2<RelatedChebiEntry, ChebiId>>>
        Tuple2<Long, Tuple2<ChebiEntry, Long>> tuple = new Tuple2<>(relatedId, chebiTuple);

        Tuple2<Long, ChebiEntry> result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(chebiId, result._1);

        ChebiEntry returnedRelatedChebiEntry = result._2;
        assertEquals(relatedChebiEntry.getId(), returnedRelatedChebiEntry.getId());
        assertEquals(relatedChebiEntry.getName(), returnedRelatedChebiEntry.getName());
        assertEquals(relatedChebiEntry.getInchiKey(), returnedRelatedChebiEntry.getInchiKey());
        assertTrue(returnedRelatedChebiEntry.getRelatedIds().isEmpty());
    }

    @Test
    void canMapRelatedEmptyReturnTheSameEntry() throws Exception {
        ChebiRelatedChebiMapper mapper = new ChebiRelatedChebiMapper();
        Long relatedId = 2222L;
        ChebiEntry relatedChebiEntry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(relatedId))
                        .name("entryName")
                        .inchiKey("InchValue")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("444").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("555").build())
                        .build();
        Long chebiId = 11111L;
        Tuple2<ChebiEntry, Long> chebiTuple = new Tuple2<>(relatedChebiEntry, chebiId);

        // Tuple2<RelatedId, Tuple2<RelatedChebiEntry, ChebiId>>>
        Tuple2<Long, Tuple2<ChebiEntry, Long>> tuple = new Tuple2<>(relatedId, chebiTuple);

        Tuple2<Long, ChebiEntry> result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(chebiId, result._1);

        ChebiEntry returnedRelatedChebiEntry = result._2;
        assertEquals(relatedChebiEntry.getId(), returnedRelatedChebiEntry.getId());
        assertEquals(relatedChebiEntry.getName(), returnedRelatedChebiEntry.getName());
        assertEquals(relatedChebiEntry.getInchiKey(), returnedRelatedChebiEntry.getInchiKey());
        assertTrue(returnedRelatedChebiEntry.getRelatedIds().isEmpty());
    }
}
