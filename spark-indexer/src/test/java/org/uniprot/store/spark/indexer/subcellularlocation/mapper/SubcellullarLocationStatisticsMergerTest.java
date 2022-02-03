package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.uniprot.store.spark.indexer.subcellularlocation.mapper.SubcellularLocationFlatAncestorTest.createSubcellularLocationEntry;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;
import org.uniprot.core.cv.subcell.SubcellularLocationEntry;
import org.uniprot.core.cv.subcell.impl.SubcellularLocationEntryBuilder;
import org.uniprot.core.impl.StatisticsBuilder;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class SubcellullarLocationStatisticsMergerTest {

    @Test
    void testMergedSubcellWithOneNull() throws Exception {
        SubcellullarLocationStatisticsMerger mapper = new SubcellullarLocationStatisticsMerger();
        SubcellularLocationEntry entry = createSubcellularLocationEntry("SL-0001");
        SubcellularLocationEntry mergedEntry = mapper.call(entry, null);
        Assertions.assertNotNull(mergedEntry);
        Assertions.assertTrue(entry == mergedEntry);
    }

    @Test
    void testMergedSubcell() throws Exception {
        SubcellullarLocationStatisticsMerger mapper = new SubcellullarLocationStatisticsMerger();
        SubcellularLocationEntry entry1 = createSubcellularLocationEntry("SL-0001");
        Statistics stats1 =
                new StatisticsBuilder().unreviewedProteinCount(3L).reviewedProteinCount(2L).build();
        SubcellularLocationEntry entryWithStats1 =
                SubcellularLocationEntryBuilder.from(entry1).statistics(stats1).build();
        SubcellularLocationEntry entry2 = createSubcellularLocationEntry("SL-0001");
        Statistics stats2 =
                new StatisticsBuilder().unreviewedProteinCount(2L).reviewedProteinCount(3L).build();
        SubcellularLocationEntry entryWithStats2 =
                SubcellularLocationEntryBuilder.from(entry2).statistics(stats2).build();
        SubcellularLocationEntry mergedEntry = mapper.call(entryWithStats1, entryWithStats2);
        Assertions.assertNotNull(mergedEntry);
        Assertions.assertEquals(entry1.getId(), mergedEntry.getId());
        Assertions.assertEquals(entry1.getName(), mergedEntry.getName());
        Assertions.assertNotNull(mergedEntry.getStatistics());
        Assertions.assertEquals(
                stats1.getReviewedProteinCount() + stats2.getReviewedProteinCount(),
                mergedEntry.getStatistics().getReviewedProteinCount());
        Assertions.assertEquals(
                stats1.getUnreviewedProteinCount() + stats2.getUnreviewedProteinCount(),
                mergedEntry.getStatistics().getUnreviewedProteinCount());
    }

    @Test
    void testMergedSubcellWithStatsMissing() throws Exception {
        SubcellullarLocationStatisticsMerger mapper = new SubcellullarLocationStatisticsMerger();
        SubcellularLocationEntry entry1 = createSubcellularLocationEntry("SL-0001");
        Statistics stats1 =
                new StatisticsBuilder().unreviewedProteinCount(3L).reviewedProteinCount(2L).build();
        SubcellularLocationEntry entryWithStats1 =
                SubcellularLocationEntryBuilder.from(entry1).statistics(stats1).build();
        SubcellularLocationEntry entry2 = createSubcellularLocationEntry("SL-0001");
        SubcellularLocationEntry mergedEntry = mapper.call(entryWithStats1, entry2);
        Assertions.assertNotNull(mergedEntry);
        Assertions.assertEquals(entry1.getId(), mergedEntry.getId());
        Assertions.assertEquals(entry1.getName(), mergedEntry.getName());
        Assertions.assertNotNull(mergedEntry.getStatistics());
        Assertions.assertEquals(
                stats1.getReviewedProteinCount(),
                mergedEntry.getStatistics().getReviewedProteinCount());
        Assertions.assertEquals(
                stats1.getUnreviewedProteinCount(),
                mergedEntry.getStatistics().getUnreviewedProteinCount());
    }
}
