package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

class OrganismJoinMapperTest {

    @Test
    void mapTremblCountUnreviewed() throws Exception {
        OrganismJoinMapper mapper = new OrganismJoinMapper();
        String entryStr =
                "ID   PRKN_HUMAN              Unreviewed;         465 AA.\n"
                        + "AC   Q8YW84;\n"
                        + "OX   NCBI_TaxID=10090;\n";
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entryStr);
        assertNotNull(result);
        assertEquals("10090", result._1);

        TaxonomyStatisticsWrapper statisticsWrapper = result._2;
        assertNotNull(statisticsWrapper);
        assertTrue(statisticsWrapper.isOrganismUnreviewedProtein());
        assertFalse(statisticsWrapper.isOrganismReviewedProtein());

        assertNotNull(statisticsWrapper.getStatistics());
        TaxonomyStatistics statistics = statisticsWrapper.getStatistics();
        assertEquals(1, statistics.getUnreviewedProteinCount());
        assertEquals(0, statistics.getReviewedProteinCount());
    }

    @Test
    void mapSwissProtCountReviewed() throws Exception {
        OrganismJoinMapper mapper = new OrganismJoinMapper();
        String entryStr =
                "ID   PRKN_HUMAN              Reviewed;         465 AA.\n"
                        + "AC   Q8YW84;\n"
                        + "OX   NCBI_TaxID=10090;\n";
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entryStr);
        assertNotNull(result);
        assertEquals("10090", result._1);

        TaxonomyStatisticsWrapper statisticsWrapper = result._2;
        assertNotNull(statisticsWrapper);
        assertTrue(statisticsWrapper.isOrganismReviewedProtein());
        assertFalse(statisticsWrapper.isOrganismUnreviewedProtein());

        assertNotNull(statisticsWrapper.getStatistics());
        TaxonomyStatistics statistics = statisticsWrapper.getStatistics();
        assertEquals(1, statistics.getReviewedProteinCount());
        assertEquals(0, statistics.getUnreviewedProteinCount());
    }

    @Test
    void mapIsoformDoNotCountStatistics() throws Exception {
        OrganismJoinMapper mapper = new OrganismJoinMapper();
        String entryStr =
                "ID   PRKN_HUMAN              Reviewed;         465 AA.\n"
                        + "AC   Q8YW84-2;\n"
                        + "OX   NCBI_TaxID=10090;\n";
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entryStr);
        assertNotNull(result);
        assertEquals("10090", result._1);

        TaxonomyStatisticsWrapper statisticsWrapper = result._2;
        assertNotNull(statisticsWrapper);
        assertFalse(statisticsWrapper.isOrganismUnreviewedProtein());
        assertFalse(statisticsWrapper.isOrganismReviewedProtein());

        assertNotNull(statisticsWrapper.getStatistics());
        TaxonomyStatistics statistics = statisticsWrapper.getStatistics();
        assertEquals(0, statistics.getUnreviewedProteinCount());
        assertEquals(0, statistics.getReviewedProteinCount());
    }
}
