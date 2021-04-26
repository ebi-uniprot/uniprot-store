package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 26/04/2021
 */
class LiteratureMappedRefStatsMapperTest {

    @Test
    void canUpdateKeyToPubMedId() throws Exception {
        LiteratureMappedRefStatsMapper mapper = new LiteratureMappedRefStatsMapper();
        Tuple2<String, Long> tuple = new Tuple2<>("ACC_PUBMEDID", 1L);

        Tuple2<String, Long> result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals("PUBMEDID", result._1);
        assertEquals(1L, result._2);
    }
}
