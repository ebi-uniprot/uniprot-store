package uk.ac.ebi.uniprot.indexer.subcell;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author lgonzales
 * @since 2019-07-18
 */
class SubcellularLocationStatisticsReaderTest {

    @Test
    void mapSubcellularLocationStatisticsCount() throws Exception {
        //when
        ResultSet result = mock(ResultSet.class);
        when(result.getString("identifier")).thenReturn("SL-0001");
        when(result.getLong("reviewedProteinCount")).thenReturn(10L);
        when(result.getLong("unreviewedProteinCount")).thenReturn(20L);

        //then
        SubcellularLocationStatisticsReader reader = new SubcellularLocationStatisticsReader();
        SubcellularLocationStatisticsReader.SubcellularLocationCount count = reader.mapRow(result, 1);

        assertNotNull(count);
        assertEquals("SL-0001", count.getSubcellularLocationId());
        assertEquals(10L, count.getReviewedProteinCount());
        assertEquals(20L, count.getUnreviewedProteinCount());
    }

}