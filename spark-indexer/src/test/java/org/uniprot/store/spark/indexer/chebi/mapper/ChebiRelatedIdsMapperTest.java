package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

class ChebiRelatedIdsMapperTest {

    @Test
    void mapChebiEntriesWithRelated() throws Exception {
        ChebiRelatedIdsMapper mapper = new ChebiRelatedIdsMapper();
        Long chebiId = 1111L;
        ChebiEntry entry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(chebiId))
                        .relatedIdsAdd(new ChebiEntryBuilder().id("222").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("333").build())
                        .build();
        Iterator<Tuple2<Long, Long>> result = mapper.call(entry);
        assertNotNull(result);
        ArrayList<Tuple2<Long, Long>> mappedRelated = new ArrayList<>();
        result.forEachRemaining(mappedRelated::add);
        assertEquals(2, mappedRelated.size());
        Tuple2<Long, Long> relatedTuple = mappedRelated.get(0);
        assertEquals(222L, relatedTuple._1);
        assertEquals(chebiId, relatedTuple._2);

        relatedTuple = mappedRelated.get(1);
        assertEquals(333L, relatedTuple._1);
        assertEquals(chebiId, relatedTuple._2);
    }

    @Test
    void mapChebiEntriesWithMajorMicrospecies() throws Exception {
        ChebiRelatedIdsMapper mapper = new ChebiRelatedIdsMapper();
        Long chebiId = 1111L;
        ChebiEntry entry =
                new ChebiEntryBuilder()
                        .id(String.valueOf(chebiId))
                        .majorMicrospeciesAdd(new ChebiEntryBuilder().id("222").build())
                        .majorMicrospeciesAdd(new ChebiEntryBuilder().id("333").build())
                        .build();
        Iterator<Tuple2<Long, Long>> result = mapper.call(entry);
        assertNotNull(result);
        ArrayList<Tuple2<Long, Long>> mappedRelated = new ArrayList<>();
        result.forEachRemaining(mappedRelated::add);
        assertEquals(4, mappedRelated.size());
        Tuple2<Long, Long> relatedTuple = mappedRelated.get(0);
        assertEquals(chebiId, relatedTuple._1);
        assertEquals(222L, relatedTuple._2);

        relatedTuple = mappedRelated.get(1);
        assertEquals(222L, relatedTuple._1);
        assertEquals(chebiId, relatedTuple._2);
        relatedTuple = mappedRelated.get(2);
        assertEquals(chebiId, relatedTuple._1);
        assertEquals(333L, relatedTuple._2);

        relatedTuple = mappedRelated.get(3);
        assertEquals(333L, relatedTuple._1);
        assertEquals(chebiId, relatedTuple._2);
    }

    @Test
    void mapChebiEntriesWithoutRelated() throws Exception {
        ChebiRelatedIdsMapper mapper = new ChebiRelatedIdsMapper();
        Long chebiId = 1111L;
        ChebiEntry entry = new ChebiEntryBuilder().id(String.valueOf(chebiId)).build();
        Iterator<Tuple2<Long, Long>> result = mapper.call(entry);
        assertNotNull(result);
        assertFalse(result.hasNext());
    }
}
