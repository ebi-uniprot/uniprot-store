package org.uniprot.store.spark.indexer.subcellularlocation.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.uniprot.core.Statistics;

/**
 * @author sahmad
 * @created 03/02/2022
 */
class StatisticsAggregationMapperTest {

    @Test
    void testMergedStats() throws Exception {
        StatisticsAggregationMapper mapper = new StatisticsAggregationMapper();
        HashSet<MappedProteinAccession> mappedProteins = new HashSet<>();
        MappedProteinAccession mpa1 =
                MappedProteinAccession.builder()
                        .proteinAccession("P12345")
                        .isReviewed(true)
                        .build();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder()
                        .proteinAccession("Q12345")
                        .isReviewed(false)
                        .build();
        MappedProteinAccession mpa3 =
                MappedProteinAccession.builder()
                        .proteinAccession("P22345")
                        .isReviewed(true)
                        .build();
        mappedProteins.addAll(Set.of(mpa1, mpa2, mpa3));
        Statistics mergedStats = mapper.call(mappedProteins);
        assertNotNull(mergedStats);
        assertEquals(2, mergedStats.getReviewedProteinCount());
        assertEquals(1, mergedStats.getUnreviewedProteinCount());
    }

    @Test
    void testMergedStatsWithoutReviewed() throws Exception {
        StatisticsAggregationMapper mapper = new StatisticsAggregationMapper();
        HashSet<MappedProteinAccession> mappedProteins = new HashSet<>();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder()
                        .proteinAccession("Q12345")
                        .isReviewed(false)
                        .build();
        mappedProteins.add(mpa2);
        Statistics mergedStats = mapper.call(mappedProteins);
        assertNotNull(mergedStats);
        assertEquals(0, mergedStats.getReviewedProteinCount());
        assertEquals(1, mergedStats.getUnreviewedProteinCount());
    }

    @Test
    void testMergedStatsWithoutUnreviewed() throws Exception {
        StatisticsAggregationMapper mapper = new StatisticsAggregationMapper();
        HashSet<MappedProteinAccession> mappedProteins = new HashSet<>();
        MappedProteinAccession mpa2 =
                MappedProteinAccession.builder()
                        .proteinAccession("Q12345")
                        .isReviewed(true)
                        .build();
        MappedProteinAccession mpa1 =
                MappedProteinAccession.builder()
                        .proteinAccession("P12345")
                        .isReviewed(true)
                        .build();
        mappedProteins.add(mpa2);
        mappedProteins.add(mpa1);
        Statistics mergedStats = mapper.call(mappedProteins);
        assertNotNull(mergedStats);
        assertEquals(2, mergedStats.getReviewedProteinCount());
        assertEquals(0, mergedStats.getUnreviewedProteinCount());
    }
}
