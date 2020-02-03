package org.uniprot.store.spark.indexer.subcell;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryImpl;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author lgonzales
 * @since 2020-01-24
 */
class SubcellularLocationMapperTest {

    @Test
    void testSubcellularLocationMapper() throws Exception {
        SubcellularLocationEntryImpl entry = new SubcellularLocationEntryImpl();
        entry.setId("ID");

        SubcellularLocationMapper mapper = new SubcellularLocationMapper();
        Tuple2<String, SubcellularLocationEntry> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("id", result._1);
        assertEquals(entry, result._2);
    }
}