package org.uniprot.store.spark.indexer.chebi;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.graphx.Edge;
import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

class GraphExtractEdgesFromVerticesMapperTest {

    @Test
    void mapWithoutRelatedIds() throws Exception {
        GraphExtractEdgesFromVerticesMapper mapper = new GraphExtractEdgesFromVerticesMapper();
        ChebiEntry entry = new ChebiEntryBuilder().id("1").build();
        Tuple2<Object, ChebiEntry> tuple = new Tuple2<>(1L, entry);
        Iterator<Edge<String>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Edge<String>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertTrue(resultList.isEmpty());
    }

    @Test
    void mapWithRelatedIds() throws Exception {
        GraphExtractEdgesFromVerticesMapper mapper = new GraphExtractEdgesFromVerticesMapper();
        List<ChebiEntry> relatedIds = new ArrayList<>();
        relatedIds.add(new ChebiEntryBuilder().id("2").build());
        relatedIds.add(new ChebiEntryBuilder().id("3").build());
        ChebiEntry entry = new ChebiEntryBuilder().id("1").relatedIdsSet(relatedIds).build();
        Tuple2<Object, ChebiEntry> tuple = new Tuple2<>(1L, entry);
        Iterator<Edge<String>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Edge<String>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());

        Edge<String> edge1 = resultList.get(0);
        assertEquals(1L, edge1.srcId());
        assertEquals(2L, edge1.dstId());
        assertEquals("isA", edge1.attr());

        Edge<String> edge2 = resultList.get(1);
        assertEquals(1L, edge2.srcId());
        assertEquals(3L, edge2.dstId());
        assertEquals("isA", edge2.attr());
    }
}
