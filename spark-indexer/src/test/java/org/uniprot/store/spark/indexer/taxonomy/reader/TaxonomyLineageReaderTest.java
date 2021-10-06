package org.uniprot.store.spark.indexer.taxonomy.reader;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.store.spark.indexer.taxonomy.reader.TaxonomyLineageReader;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class TaxonomyLineageReaderTest {

    @Test
    void getRangesRoundDown() {
        TaxonomyLineageReader reader = new TaxonomyLineageReader(null, false);
        int[][] ranges = reader.getRanges(999, 10);
        assertNotNull(ranges);
        assertEquals(1, ranges[0][0]);
        assertEquals(100, ranges[0][1]);

        assertEquals(101, ranges[1][0]);
        assertEquals(200, ranges[1][1]);

        assertEquals(901, ranges[9][0]);
        assertEquals(999, ranges[9][1]);
    }

    @Test
    void getRangesExact() {
        TaxonomyLineageReader reader = new TaxonomyLineageReader(null, false);
        int[][] ranges = reader.getRanges(1000, 10);
        assertNotNull(ranges);

        assertEquals(1, ranges[0][0]);
        assertEquals(100, ranges[0][1]);

        assertEquals(101, ranges[1][0]);
        assertEquals(200, ranges[1][1]);

        assertEquals(901, ranges[9][0]);
        assertEquals(1000, ranges[9][1]);
    }

    @Test
    void getRangesRoundUp() {
        TaxonomyLineageReader reader = new TaxonomyLineageReader(null, false);
        int[][] ranges = reader.getRanges(1001, 10);
        assertNotNull(ranges);

        assertEquals(1, ranges[0][0]);
        assertEquals(101, ranges[0][1]);

        assertEquals(102, ranges[1][0]);
        assertEquals(202, ranges[1][1]);

        assertEquals(910, ranges[9][0]);
        assertEquals(1001, ranges[9][1]);
    }
}
