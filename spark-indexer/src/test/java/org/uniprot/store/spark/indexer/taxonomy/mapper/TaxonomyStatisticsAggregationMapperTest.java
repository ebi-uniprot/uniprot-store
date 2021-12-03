package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

class TaxonomyStatisticsAggregationMapperTest {

    @Test
    void aggregateStatOneOnly() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat1 =
                new TaxonomyStatisticsBuilder()
                        .referenceProteomeCount(1)
                        .proteomeCount(2)
                        .reviewedProteinCount(3)
                        .unreviewedProteinCount(4)
                        .build();

        TaxonomyStatisticsWrapper wrapper1 =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(stat1)
                        .organismUnreviewedProtein(true)
                        .organismReviewedProtein(false)
                        .build();

        TaxonomyStatisticsWrapper result = mapper.call(wrapper1, null);
        assertNotNull(result);
        assertEquals(wrapper1, result);
    }

    @Test
    void aggregateStatTwoOnly() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat2 =
                new TaxonomyStatisticsBuilder()
                        .referenceProteomeCount(1)
                        .proteomeCount(2)
                        .reviewedProteinCount(3)
                        .unreviewedProteinCount(4)
                        .build();

        TaxonomyStatisticsWrapper wrapper2 =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(stat2)
                        .organismUnreviewedProtein(false)
                        .organismReviewedProtein(true)
                        .build();

        TaxonomyStatisticsWrapper result = mapper.call(null, wrapper2);
        assertNotNull(result);
        assertEquals(wrapper2, result);
    }

    @Test
    void aggregateMerge() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat1 =
                new TaxonomyStatisticsBuilder()
                        .referenceProteomeCount(1)
                        .proteomeCount(2)
                        .reviewedProteinCount(3)
                        .unreviewedProteinCount(4)
                        .build();
        TaxonomyStatisticsWrapper wrapper1 =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(stat1)
                        .organismUnreviewedProtein(false)
                        .organismReviewedProtein(true)
                        .build();

        TaxonomyStatistics stat2 =
                new TaxonomyStatisticsBuilder()
                        .referenceProteomeCount(5)
                        .proteomeCount(6)
                        .reviewedProteinCount(7)
                        .unreviewedProteinCount(8)
                        .build();

        TaxonomyStatisticsWrapper wrapper2 =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(stat2)
                        .organismUnreviewedProtein(true)
                        .organismReviewedProtein(false)
                        .build();

        TaxonomyStatisticsWrapper result = mapper.call(wrapper1, wrapper2);
        assertNotNull(result);
        assertTrue(result.isOrganismUnreviewedProtein());
        assertTrue(result.isOrganismReviewedProtein());

        TaxonomyStatistics resultStat = result.getStatistics();
        assertNotNull(resultStat);
        assertEquals(12L, resultStat.getUnreviewedProteinCount());
        assertEquals(10L, resultStat.getReviewedProteinCount());
        assertEquals(8L, resultStat.getProteomeCount());
        assertEquals(6L, resultStat.getReferenceProteomeCount());
    }
}
