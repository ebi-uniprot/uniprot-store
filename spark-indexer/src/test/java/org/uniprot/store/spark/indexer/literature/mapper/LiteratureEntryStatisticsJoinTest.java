package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 30/03/2021
 */
class LiteratureEntryStatisticsJoinTest {

    @Test
    void mapperCommunityWithoutCount() throws Exception {
        LiteratureEntryStatisticsJoin joinMapper =
                new LiteratureEntryStatisticsJoin(
                        LiteratureEntryStatisticsJoin.StatisticsType.COMMUNITY);

        LiteratureEntry entry =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();
        Optional<Long> count = Optional.empty();
        Tuple2<LiteratureEntry, Optional<Long>> tuple = new Tuple2<>(entry, count);
        LiteratureEntry result = joinMapper.call(tuple);

        assertNotNull(result);
        assertEquals(entry, result);
    }

    @Test
    void mapperCommunityWithCount() throws Exception {
        LiteratureEntryStatisticsJoin joinMapper =
                new LiteratureEntryStatisticsJoin(
                        LiteratureEntryStatisticsJoin.StatisticsType.COMMUNITY);

        LiteratureEntry entry =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder().reviewedProteinCount(10).build())
                        .build();
        Optional<Long> count = Optional.of(10L);
        Tuple2<LiteratureEntry, Optional<Long>> tuple = new Tuple2<>(entry, count);
        LiteratureEntry result = joinMapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.getStatistics());
        LiteratureStatistics statistics = result.getStatistics();
        assertEquals(10L, statistics.getReviewedProteinCount());
        assertEquals(10L, statistics.getCommunityMappedProteinCount());
    }

    @Test
    void mapperComputationallyWithoutCount() throws Exception {
        LiteratureEntryStatisticsJoin joinMapper =
                new LiteratureEntryStatisticsJoin(
                        LiteratureEntryStatisticsJoin.StatisticsType.COMMUNITY);

        LiteratureEntry entry =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();
        Optional<Long> count = Optional.empty();
        Tuple2<LiteratureEntry, Optional<Long>> tuple = new Tuple2<>(entry, count);
        LiteratureEntry result = joinMapper.call(tuple);

        assertNotNull(result);
        assertEquals(entry, result);
    }

    @Test
    void mapperComputationallyWithCount() throws Exception {
        LiteratureEntryStatisticsJoin joinMapper =
                new LiteratureEntryStatisticsJoin(
                        LiteratureEntryStatisticsJoin.StatisticsType.COMPUTATIONALLY);

        LiteratureEntry entry =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder().reviewedProteinCount(10).build())
                        .build();
        Optional<Long> count = Optional.of(10L);
        Tuple2<LiteratureEntry, Optional<Long>> tuple = new Tuple2<>(entry, count);
        LiteratureEntry result = joinMapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.getStatistics());
        LiteratureStatistics statistics = result.getStatistics();
        assertEquals(10L, statistics.getReviewedProteinCount());
        assertEquals(10L, statistics.getComputationallyMappedProteinCount());
    }
}
