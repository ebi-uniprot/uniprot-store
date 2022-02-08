package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationFlatAncestorTest.createSubcellularLocationEntry;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.impl.StatisticsBuilder;

import scala.Tuple2;

/**
 * @author sahmad
 * @created 08/02/2022
 */
class SubcellularLocationEntryStatisticsMergerTest {

    @Test
    void testMergeStatsToEntry() throws Exception {
        SubcellularLocationEntryStatisticsMerger merger =
                new SubcellularLocationEntryStatisticsMerger();
        SubcellularLocationEntry entryWithoutStats = createSubcellularLocationEntry("SL-1234");
        Assertions.assertNull(entryWithoutStats.getStatistics());
        Statistics statistics =
                new StatisticsBuilder()
                        .reviewedProteinCount(100L)
                        .unreviewedProteinCount(200L)
                        .build();
        Tuple2<SubcellularLocationEntry, Optional<Statistics>> tuple =
                new Tuple2<>(entryWithoutStats, Optional.of(statistics));
        SubcellularLocationEntry entryWithStats = merger.call(tuple);
        Assertions.assertNotNull(entryWithStats);
        Assertions.assertEquals(entryWithoutStats.getId(), entryWithStats.getId());
        Assertions.assertNotNull(entryWithStats.getStatistics());
        Assertions.assertEquals(
                statistics.getReviewedProteinCount(),
                entryWithStats.getStatistics().getReviewedProteinCount());
        Assertions.assertEquals(
                statistics.getUnreviewedProteinCount(),
                entryWithStats.getStatistics().getUnreviewedProteinCount());
    }

    @Test
    void testMergeEmptyStatsToEntry() throws Exception {
        SubcellularLocationEntryStatisticsMerger merger =
                new SubcellularLocationEntryStatisticsMerger();
        SubcellularLocationEntry entryWithoutStats = createSubcellularLocationEntry("SL-2234");
        Assertions.assertNull(entryWithoutStats.getStatistics());
        Tuple2<SubcellularLocationEntry, Optional<Statistics>> tuple =
                new Tuple2<>(entryWithoutStats, Optional.empty());
        SubcellularLocationEntry entryWithStats = merger.call(tuple);
        Assertions.assertNotNull(entryWithStats);
        Assertions.assertEquals(entryWithoutStats.getId(), entryWithStats.getId());
        Assertions.assertNull(entryWithStats.getStatistics());
    }
}
