package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;
import org.uniprot.core.impl.StatisticsBuilder;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class StatisticsAggregationMapperTest {

    @Test
    void testMergedStatsWithOneNullStats() throws Exception {
        StatisticsAggregationMapper mapper = new StatisticsAggregationMapper();
        Statistics stats1 = new StatisticsBuilder().build();
        Statistics mergedStats = mapper.call(stats1, null);
        Assertions.assertTrue(stats1 == mergedStats);
    }

    @Test
    void testMergedStats() throws Exception {
        StatisticsAggregationMapper mapper = new StatisticsAggregationMapper();
        Statistics stats1 = new StatisticsBuilder().reviewedProteinCount(2L).build();
        Statistics stats2 = new StatisticsBuilder().reviewedProteinCount(2L).unreviewedProteinCount(3).build();
        Statistics mergedStats = mapper.call(stats1, stats2);
        Assertions.assertNotNull(mergedStats);
        Assertions.assertEquals(4L, mergedStats.getReviewedProteinCount());
        Assertions.assertEquals(3L, mergedStats.getUnreviewedProteinCount());
    }
}
