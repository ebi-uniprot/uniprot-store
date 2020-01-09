package indexer.taxonomy;

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
        assertEquals(ranges[0][0], 1);
        assertEquals(ranges[0][1], 100);

        assertEquals(ranges[1][0], 101);
        assertEquals(ranges[1][1], 200);

        assertEquals(ranges[9][0], 901);
        assertEquals(ranges[9][1], 999);
    }

    @Test
    void getRangesExact() {
        int[][] ranges = TaxonomyLineageReader.getRanges(1000, 10);
        assertNotNull(ranges);

        assertEquals(ranges[0][0], 1);
        assertEquals(ranges[0][1], 100);

        assertEquals(ranges[1][0], 101);
        assertEquals(ranges[1][1], 200);

        assertEquals(ranges[9][0], 901);
        assertEquals(ranges[9][1], 1000);
    }

    @Test
    void getRangesRoundUp() {
        int[][] ranges = TaxonomyLineageReader.getRanges(1001, 10);
        assertNotNull(ranges);

        assertEquals(ranges[0][0], 1);
        assertEquals(ranges[0][1], 101);

        assertEquals(ranges[1][0], 102);
        assertEquals(ranges[1][1], 202);

        assertEquals(ranges[9][0], 910);
        assertEquals(ranges[9][1], 1001);
    }
}
