package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class UniParcSequenceSourceMapperTest {

    @Test
    void mapValidLine() throws Exception {
        UniParcSequenceSourceMapper mapper = new UniParcSequenceSourceMapper();
        Tuple2<String, Set<String>> result = mapper.call("I8FBX0\tCAC20866\tEMBL\tN\t3");
        assertNotNull(result);
        assertEquals("I8FBX0", result._1);
        assertEquals(Set.of("CAC20866"), result._2);
    }

    @Test
    void mapInvalidLine() {
        UniParcSequenceSourceMapper mapper = new UniParcSequenceSourceMapper();
        SparkIndexException error =
                assertThrows(SparkIndexException.class, () -> mapper.call("INVALID"));
        assertNotNull(error);
        assertEquals("Unable to parse UniParcSourceMapper line: INVALID", error.getMessage());
    }
}
