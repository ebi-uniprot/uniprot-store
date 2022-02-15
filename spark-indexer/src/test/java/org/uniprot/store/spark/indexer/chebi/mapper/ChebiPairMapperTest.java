package org.uniprot.store.spark.indexer.chebi.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.chebi.ChebiEntry;
import org.uniprot.core.cv.chebi.impl.ChebiEntryBuilder;

import scala.Tuple2;

class ChebiPairMapperTest {

    @Test
    void canMapChebiVerticesToPairRDD() throws Exception {
        ChebiPairMapper mapper = new ChebiPairMapper();
        ChebiEntry entry = new ChebiEntryBuilder().id("1").build();
        Tuple2<Object, ChebiEntry> tuple = new Tuple2<>(1L, entry);
        Tuple2<String, ChebiEntry> result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals("1", result._1);
        assertEquals(entry, result._2);
    }
}
