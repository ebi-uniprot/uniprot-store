package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.graphx.EdgeTriplet;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;

class GraphSendVertexMessageMapperTest {

    @Test
    void canSendVerticesMessage() {
        GraphSendVertexMessageMapper mapper = new GraphSendVertexMessageMapper();

        ChebiEntry srcChebi =
                new ChebiEntryBuilder()
                        .id("1")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("2").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("3").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("4").build())
                        .build();

        ChebiEntry dstChebi =
                new ChebiEntryBuilder()
                        .id("20")
                        .relatedIdsAdd(new ChebiEntryBuilder().id("20").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("30").build())
                        .relatedIdsAdd(new ChebiEntryBuilder().id("40").build())
                        .build();

        EdgeTriplet<ChebiEntry, String> triplet = Mockito.mock(EdgeTriplet.class);
        Mockito.when(triplet.srcId()).thenReturn(1L);
        Mockito.when(triplet.dstAttr()).thenReturn(srcChebi);
        Mockito.when(triplet.srcAttr()).thenReturn(dstChebi);
        Iterator<Tuple2<Object, ChebiEntry>> result = mapper.apply(triplet);
        assertNotNull(result);
        List<Tuple2<Object, ChebiEntry>> resultList = new ArrayList<>();
        JavaConversions.asJavaIterator(result).forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(1, resultList.size());

        Tuple2<Object, ChebiEntry> tuple = resultList.get(0);
        assertEquals(1L, tuple._1);

        ChebiEntry chebiEntry = tuple._2;
        assertNotNull(chebiEntry);
        List<String> relatedIds =
                chebiEntry.getRelatedIds().stream()
                        .map(ChebiEntry::getId)
                        .collect(Collectors.toList());
        assertTrue(relatedIds.contains("2"));
        assertTrue(relatedIds.contains("3"));
        assertTrue(relatedIds.contains("4"));

        assertTrue(relatedIds.contains("20"));
        assertTrue(relatedIds.contains("30"));
        assertTrue(relatedIds.contains("40"));
    }
}
