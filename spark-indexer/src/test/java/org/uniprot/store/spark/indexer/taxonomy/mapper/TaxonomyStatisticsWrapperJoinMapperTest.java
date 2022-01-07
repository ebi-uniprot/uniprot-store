package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

class TaxonomyStatisticsWrapperJoinMapperTest {

    @Test
    void callWithBothEmpty() throws Exception {
        TaxonomyStatisticsWrapperJoinMapper mapper = new TaxonomyStatisticsWrapperJoinMapper();
        Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(Optional.empty(), Optional.empty());
        TaxonomyStatisticsWrapper result = mapper.call(tuple);
        assertNotNull(result);
        TaxonomyStatisticsWrapper emptyWrapper =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(new TaxonomyStatisticsBuilder().build())
                        .build();
        assertEquals(emptyWrapper, result);
    }

    @Test
    void callOnlyFirstWrapper() throws Exception {
        TaxonomyStatisticsWrapperJoinMapper mapper = new TaxonomyStatisticsWrapperJoinMapper();

        TaxonomyStatisticsWrapper wrapper1 = getProteinStatisticsWrapper();

        Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(Optional.of(wrapper1), Optional.empty());
        TaxonomyStatisticsWrapper result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(wrapper1, result);
    }

    @Test
    void callOnlySecondWrapper() throws Exception {
        TaxonomyStatisticsWrapperJoinMapper mapper = new TaxonomyStatisticsWrapperJoinMapper();

        TaxonomyStatisticsWrapper wrapper2 = getProteomeStatisticWrapper();

        Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(Optional.empty(), Optional.of(wrapper2));
        TaxonomyStatisticsWrapper result = mapper.call(tuple);
        assertNotNull(result);
    }

    @Test
    void callWithBothWrappers() throws Exception {
        TaxonomyStatisticsWrapperJoinMapper mapper = new TaxonomyStatisticsWrapperJoinMapper();

        TaxonomyStatisticsWrapper wrapper1 = getProteinStatisticsWrapper();
        TaxonomyStatisticsWrapper wrapper2 = getProteomeStatisticWrapper();

        Tuple2<Optional<TaxonomyStatisticsWrapper>, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(Optional.of(wrapper1), Optional.of(wrapper2));
        TaxonomyStatisticsWrapper result = mapper.call(tuple);

        assertNotNull(result);
        assertTrue(result.isOrganismUnreviewedProtein());
        assertTrue(result.isOrganismReviewedProtein());

        TaxonomyStatistics mergedStatistic = result.getStatistics();
        assertNotNull(mergedStatistic);
        assertEquals(1L, mergedStatistic.getReviewedProteinCount());
        assertEquals(2L, mergedStatistic.getUnreviewedProteinCount());
        assertEquals(3L, mergedStatistic.getProteomeCount());
        assertEquals(4L, mergedStatistic.getReferenceProteomeCount());
    }

    private TaxonomyStatisticsWrapper getProteinStatisticsWrapper() {
        TaxonomyStatistics stat1 =
                new TaxonomyStatisticsBuilder()
                        .reviewedProteinCount(1L)
                        .unreviewedProteinCount(2L)
                        .build();

        return TaxonomyStatisticsWrapper.builder()
                .statistics(stat1)
                .organismReviewedProtein(true)
                .organismUnreviewedProtein(true)
                .build();
    }

    private TaxonomyStatisticsWrapper getProteomeStatisticWrapper() {
        TaxonomyStatistics stat2 =
                new TaxonomyStatisticsBuilder()
                        .proteomeCount(3L)
                        .referenceProteomeCount(4L)
                        .build();

        return TaxonomyStatisticsWrapper.builder()
                .statistics(stat2)
                .organismReviewedProtein(true)
                .organismUnreviewedProtein(true)
                .build();
    }
}
