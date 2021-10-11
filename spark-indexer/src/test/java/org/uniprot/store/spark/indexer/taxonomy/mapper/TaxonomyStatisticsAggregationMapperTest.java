package org.uniprot.store.spark.indexer.taxonomy.mapper;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyStatisticsAggregationMapperTest {

    @Test
    void aggregateStatOneOnly() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat1 = new TaxonomyStatisticsBuilder()
                .referenceProteomeCount(1)
                .proteomeCount(2)
                .reviewedProteinCount(3)
                .unreviewedProteinCount(4)
                .build();
        TaxonomyStatistics result = mapper.call(stat1, null);
        assertNotNull(result);
        assertEquals(stat1, result);
    }

    @Test
    void aggregateStatTwoOnly() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat2 = new TaxonomyStatisticsBuilder()
                .referenceProteomeCount(1)
                .proteomeCount(2)
                .reviewedProteinCount(3)
                .unreviewedProteinCount(4)
                .build();
        TaxonomyStatistics result = mapper.call(null, stat2);
        assertNotNull(result);
        assertEquals(stat2, result);
    }

    @Test
    void aggregateMerge() throws Exception {
        TaxonomyStatisticsAggregationMapper mapper = new TaxonomyStatisticsAggregationMapper();
        TaxonomyStatistics stat1 = new TaxonomyStatisticsBuilder()
                .referenceProteomeCount(1)
                .proteomeCount(1)
                .reviewedProteinCount(1)
                .unreviewedProteinCount(1)
                .build();
        TaxonomyStatistics stat2 = new TaxonomyStatisticsBuilder()
                .referenceProteomeCount(2)
                .proteomeCount(2)
                .reviewedProteinCount(2)
                .unreviewedProteinCount(2)
                .build();
        TaxonomyStatistics result = mapper.call(stat1, stat2);
        assertNotNull(result);
        assertEquals(3L, result.getUnreviewedProteinCount());
        assertEquals(3L, result.getReviewedProteinCount());
        assertEquals(3L, result.getReferenceProteomeCount());
        assertEquals(3L, result.getProteomeCount());
    }
}