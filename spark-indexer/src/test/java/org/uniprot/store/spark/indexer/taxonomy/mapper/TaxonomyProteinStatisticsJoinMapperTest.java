package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;

import scala.Tuple2;

class TaxonomyProteinStatisticsJoinMapperTest {

    @Test
    void canMapProteins() throws Exception {
        TaxonomyProteinStatisticsJoinMapper mapper = new TaxonomyProteinStatisticsJoinMapper();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().build();
        TaxonomyStatistics stat =
                new TaxonomyStatisticsBuilder()
                        .reviewedProteinCount(10)
                        .unreviewedProteinCount(20)
                        .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple =
                new Tuple2<>(entry, Optional.of(stat));
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(stat, result.getStatistics());
    }

    @Test
    void canMapProteinsWithProteomes() throws Exception {
        TaxonomyProteinStatisticsJoinMapper mapper = new TaxonomyProteinStatisticsJoinMapper();
        TaxonomyStatistics entryStat =
                new TaxonomyStatisticsBuilder().proteomeCount(2).referenceProteomeCount(1).build();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().statistics(entryStat).build();
        TaxonomyStatistics stat =
                new TaxonomyStatisticsBuilder()
                        .reviewedProteinCount(10)
                        .unreviewedProteinCount(20)
                        .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple =
                new Tuple2<>(entry, Optional.of(stat));
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getStatistics());
        assertEquals(2L, result.getStatistics().getProteomeCount());
        assertEquals(1L, result.getStatistics().getReferenceProteomeCount());
        assertEquals(10L, result.getStatistics().getReviewedProteinCount());
        assertEquals(20L, result.getStatistics().getUnreviewedProteinCount());
    }

    @Test
    void canMapEmpty() throws Exception {
        TaxonomyProteinStatisticsJoinMapper mapper = new TaxonomyProteinStatisticsJoinMapper();
        TaxonomyStatistics entryStat =
                new TaxonomyStatisticsBuilder().proteomeCount(2).referenceProteomeCount(1).build();
        TaxonomyEntry entry = new TaxonomyEntryBuilder().statistics(entryStat).build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple =
                new Tuple2<>(entry, Optional.empty());
        TaxonomyEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertEquals(entry, result);
    }
}
