package org.uniprot.store.spark.indexer.proteome.mapper;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.samePropertyValuesAs;

class ProteomeStatisticsAggregationMapperTest {
    private static final long REVIEWED_COUNT_0 = 45L;
    private static final long REVIEWED_COUNT_1 = 999L;
    private static final long UNREVIEWED_COUNT_0 = 7088L;
    private static final long UNREVIEWED_COUNT_1 = 10L;
    private static final long ISOFORM_COUNT_0 = 1L;
    private static final long ISOFORM_COUNT_1 = 0L;
    private static final ProteomeStatistics PROTEOME_STATISTICS_0 = new ProteomeStatisticsBuilder()
            .reviewedProteinCount(REVIEWED_COUNT_0).unreviewedProteinCount(UNREVIEWED_COUNT_0).isoformProteinCount(ISOFORM_COUNT_0).build();
    private static final ProteomeStatistics PROTEOME_STATISTICS_1 = new ProteomeStatisticsBuilder()
            .reviewedProteinCount(REVIEWED_COUNT_1).unreviewedProteinCount(UNREVIEWED_COUNT_1).isoformProteinCount(ISOFORM_COUNT_1).build();
    private final ProteomeStatisticsAggregationMapper proteomeStatisticsAggregationMapper = new ProteomeStatisticsAggregationMapper();


    @Test
    void call() throws Exception {
        ProteomeStatistics result = proteomeStatisticsAggregationMapper.call(PROTEOME_STATISTICS_0, PROTEOME_STATISTICS_1);
        assertThat(result, samePropertyValuesAs(new ProteomeStatisticsBuilder()
                .reviewedProteinCount(REVIEWED_COUNT_0 + REVIEWED_COUNT_1)
                .unreviewedProteinCount(UNREVIEWED_COUNT_0 + UNREVIEWED_COUNT_1)
                .isoformProteinCount(ISOFORM_COUNT_0 + ISOFORM_COUNT_1).build()));
    }
}