package org.uniprot.store.spark.indexer.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * @author lgonzales
 * @since 2019-12-24
 */
class TaxonomyLineageReaderTest {

    @Test
    void getRangesRoundDown() {
        int[][] ranges = TaxonomyLineageReader.getRanges(999, 10);
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
        int[][] ranges = TaxonomyLineageReader.getRanges(1000, 10);
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
        int[][] ranges = TaxonomyLineageReader.getRanges(1001, 10);
        assertNotNull(ranges);

        assertEquals(1, ranges[0][0]);
        assertEquals(101, ranges[0][1]);

        assertEquals(102, ranges[1][0]);
        assertEquals(202, ranges[1][1]);

        assertEquals(910, ranges[9][0]);
        assertEquals(1001, ranges[9][1]);
    }
}
