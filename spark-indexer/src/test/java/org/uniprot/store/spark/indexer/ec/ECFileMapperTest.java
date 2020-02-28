package org.uniprot.store.spark.indexer.ec;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.ec.ECEntry;
import org.uniprot.core.cv.ec.builder.ECEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class ECFileMapperTest {

    @Test
    void testECFileMapper() throws Exception {
        ECEntry entry = new ECEntryBuilder().id("ecID").label("ecLabel").build();

        ECFileMapper mapper = new ECFileMapper();
        Tuple2<String, ECEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("ecID", result._1);
        assertEquals(entry, result._2);
    }
}
