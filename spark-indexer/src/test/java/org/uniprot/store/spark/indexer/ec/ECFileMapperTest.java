package org.uniprot.store.spark.indexer.ec;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.ec.EC;
import org.uniprot.core.cv.ec.impl.ECImpl;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class ECFileMapperTest {

    @Test
    void testECFileMapper() throws Exception {
        ECImpl entry = new ECImpl("ecID", "ecLabel");

        ECFileMapper mapper = new ECFileMapper();
        Tuple2<String, EC> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("ecID", result._1);
        assertEquals(entry, result._2);
    }
}
