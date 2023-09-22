package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class UniProtIdTrackerMapperTest {

    @Test
    void canMapLine() throws Exception {
        UniProtIdTrackerMapper mapper = new UniProtIdTrackerMapper();
        Tuple2<String, Set<String>> result = mapper.call("accession\tuniprotId");
        assertNotNull(result);
        assertEquals("accession", result._1);
        assertEquals(1, result._2.size());
        assertTrue(result._2.contains("uniprotId"));
    }

    @Test
    void nullLineThrowsException() throws Exception {
        UniProtIdTrackerMapper mapper = new UniProtIdTrackerMapper();
        assertThrows(SparkIndexException.class, () -> mapper.call(null));
    }

    @Test
    void invalidLineThrowsException() throws Exception {
        UniProtIdTrackerMapper mapper = new UniProtIdTrackerMapper();
        assertThrows(SparkIndexException.class, () -> mapper.call("InvalidLine"));
    }
}
