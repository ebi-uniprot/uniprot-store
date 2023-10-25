package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;

import scala.Tuple2;

class ProteomeStatisticsToProteomeEntryMapperTest {
    private static final int REVIEWED_PROTEIN_COUNT = 24;
    private static final int UNREVIEWED_PROTEIN_COUNT = 999;
    private static final int REFERENCE_PROTEOME_COUNT = 7;
    private final ProteomeStatisticsToProteomeEntryMapper proteomeStatisticsToProteomeEntryMapper =
            new ProteomeStatisticsToProteomeEntryMapper();
    private final ProteomeEntry proteomeEntry = new ProteomeEntryBuilder().build();

    @Test
    void call() throws Exception {
        ProteomeStatistics proteomeStatistics =
                new ProteomeStatisticsBuilder()
                        .reviewedProteinCount(REVIEWED_PROTEIN_COUNT)
                        .unreviewedProteinCount(UNREVIEWED_PROTEIN_COUNT)
                        .isoformProteinCount(REFERENCE_PROTEOME_COUNT)
                        .build();
        ProteomeEntry proteomeEntryResult =
                proteomeStatisticsToProteomeEntryMapper.call(
                        Tuple2.apply(proteomeEntry, Optional.of(proteomeStatistics)));
        assertProteomeEntry(
                proteomeEntryResult,
                REVIEWED_PROTEIN_COUNT,
                UNREVIEWED_PROTEIN_COUNT,
                REFERENCE_PROTEOME_COUNT);
    }

    private void assertProteomeEntry(
            ProteomeEntry proteomeEntryResult,
            int reviewedProteinCount,
            int unreviewedProteinCount,
            int isoformProteinCount) {
        assertEquals(
                proteomeEntryResult.getProteomeStatistics().getReviewedProteinCount(),
                reviewedProteinCount);
        assertEquals(
                proteomeEntryResult.getProteomeStatistics().getUnreviewedProteinCount(),
                unreviewedProteinCount);
        assertEquals(
                proteomeEntryResult.getProteomeStatistics().getIsoformProteinCount(),
                isoformProteinCount);
    }

    @Test
    void call_whenStatisticsNotPresent() throws Exception {
        ProteomeEntry proteomeEntryResult =
                proteomeStatisticsToProteomeEntryMapper.call(
                        Tuple2.apply(proteomeEntry, Optional.empty()));
        assertProteomeEntry(proteomeEntryResult, 0, 0, 0);
    }
}
