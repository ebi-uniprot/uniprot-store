package org.uniprot.store.spark.indexer.literature.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.literature.LiteratureEntry;
import org.uniprot.core.literature.LiteratureStatistics;
import org.uniprot.core.literature.impl.LiteratureEntryBuilder;
import org.uniprot.core.literature.impl.LiteratureStatisticsBuilder;

/**
 * @author lgonzales
 * @since 26/03/2021
 */
class LiteratureEntryAggregationMapperTest {

    @Test
    void mapperCanMergeStatistics() throws Exception {
        LiteratureEntryAggregationMapper mapper = new LiteratureEntryAggregationMapper();

        LiteratureEntry entry1 =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();
        LiteratureEntry entry2 =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();

        LiteratureEntry result = mapper.call(entry1, entry2);

        assertNotNull(result);
        assertNotNull(result.getStatistics());
        LiteratureStatistics statistics = result.getStatistics();
        assertEquals(20, statistics.getReviewedProteinCount());
        assertEquals(20, statistics.getUnreviewedProteinCount());
    }

    @Test
    void mapperEntry2Null() throws Exception {
        LiteratureEntryAggregationMapper mapper = new LiteratureEntryAggregationMapper();

        LiteratureEntry entry1 =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();

        LiteratureEntry result = mapper.call(entry1, null);

        assertNotNull(result);
        assertEquals(entry1, result);
    }

    @Test
    void mapperEntry1Null() throws Exception {
        LiteratureEntryAggregationMapper mapper = new LiteratureEntryAggregationMapper();

        LiteratureEntry entry2 =
                new LiteratureEntryBuilder()
                        .statistics(
                                new LiteratureStatisticsBuilder()
                                        .unreviewedProteinCount(10)
                                        .reviewedProteinCount(10)
                                        .build())
                        .build();

        LiteratureEntry result = mapper.call(null, entry2);

        assertNotNull(result);
        assertEquals(entry2, result);
    }
}
