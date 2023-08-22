package org.uniprot.store.spark.indexer.proteome.mapper;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeStatistics;
import org.uniprot.core.proteome.impl.ProteomeStatisticsBuilder;
import org.uniprot.store.search.document.proteome.ProteomeDocument;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatisticsToProteomeDocumentMapperTest {
    public static final int REVIEWED_PROTEIN_COUNT = 24;
    public static final int UNREVIEWED_PROTEIN_COUNT = 999;
    public static final int REFERENCE_PROTEOME_COUNT = 7;
    private final StatisticsToProteomeDocumentMapper statisticsToProteomeDocumentMapper = new StatisticsToProteomeDocumentMapper();
    private final ProteomeDocument proteomeDocument = new ProteomeDocument();

    @Test
    void call() throws Exception {
        ProteomeStatistics proteomeStatistics = new ProteomeStatisticsBuilder().reviewedProteinCount(REVIEWED_PROTEIN_COUNT)
                .unreviewedProteinCount(UNREVIEWED_PROTEIN_COUNT)
                .isoformProteinCount(REFERENCE_PROTEOME_COUNT)
                .build();
        ProteomeDocument proteomeDocumentResult = statisticsToProteomeDocumentMapper.call(Tuple2.apply(proteomeDocument, Optional.of(proteomeStatistics)));
        assertProteomeDocument(proteomeDocumentResult, REVIEWED_PROTEIN_COUNT, UNREVIEWED_PROTEIN_COUNT, REFERENCE_PROTEOME_COUNT);
    }

    private void assertProteomeDocument(ProteomeDocument proteomeDocumentResult, int reviewedProteinCount, int unreviewedProteinCount, int isoformProteinCount) {
        assertEquals(proteomeDocumentResult.reviewedProteinCount, reviewedProteinCount);
        assertEquals(proteomeDocumentResult.unreviewedProteinCount, unreviewedProteinCount);
        assertEquals(proteomeDocumentResult.isoformProteinCount, isoformProteinCount);
    }

    @Test
    void call_whenStatisticsNotPresent() throws Exception {
        ProteomeDocument proteomeDocumentResult = statisticsToProteomeDocumentMapper.call(Tuple2.apply(proteomeDocument, Optional.empty()));
        assertProteomeDocument(proteomeDocumentResult, 0, 0, 0);
    }
}