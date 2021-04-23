package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class LiteraturePubmedFileMapperTest {

    @Test
    void canMapLiteraturePubmedFile() throws Exception {
        String input = "D3Z654\tMGI\t11203701\t96770\t[Expression][Sequences]";

        LiteraturePubmedFileMapper mapper = new LiteraturePubmedFileMapper();

        Tuple2<String, Tuple2<String, String>> tuple = mapper.call(input);

        assertNotNull(tuple);
        assertNotNull(tuple._1);
        assertEquals("D3Z654", tuple._1); // accession

        assertNotNull(tuple._2);
        assertEquals("MGI", tuple._2._1); // source type
        assertEquals("11203701", tuple._2._2); // pubmedId
    }

    @Test
    void testInvalidEntry() {
        LiteraturePubmedFileMapper mapper = new LiteraturePubmedFileMapper();
        assertThrows(
                NullPointerException.class,
                () -> {
                    mapper.call(null);
                },
                "NullPointerException");
    }
}
