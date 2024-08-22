package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.common.exception.SparkIndexException;

import scala.Tuple2;

class UniProtKBUniParcMapperTest {

    @Test
    void callValidLine() throws Exception {
        UniProtKBUniParcMapper mapper = new UniProtKBUniParcMapper();
        Tuple2<String, String> tuple = mapper.call("UNIPARC ACCESSION Y");
        assertNotNull(tuple);
        assertEquals("ACCESSION", tuple._1);
        assertEquals("UNIPARC", tuple._2);
    }

    @Test
    void callInvalidLineThrowsException() {
        UniProtKBUniParcMapper mapper = new UniProtKBUniParcMapper();
        SparkIndexException error =
                assertThrows(SparkIndexException.class, () -> mapper.call("INVALID"));
        assertNotNull(error);
        assertEquals("Unable to parse UniParcUniProtKBMapper line: INVALID", error.getMessage());
    }
}
