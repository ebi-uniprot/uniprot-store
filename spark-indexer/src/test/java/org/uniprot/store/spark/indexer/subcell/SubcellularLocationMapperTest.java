package org.uniprot.store.spark.indexer.subcell;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class SubcellularLocationMapperTest {

    @Test
    void testSubcellularLocationMapper() throws Exception {
        SubcellularLocationEntry entry = new SubcellularLocationEntryBuilder().name("ID").build();

        SubcellularLocationMapper mapper = new SubcellularLocationMapper();
        Tuple2<String, SubcellularLocationEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("id", result._1);
        assertEquals(entry, result._2);
    }
}
