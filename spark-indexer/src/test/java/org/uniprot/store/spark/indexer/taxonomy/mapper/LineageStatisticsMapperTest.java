package org.uniprot.store.spark.indexer.taxonomy.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

class LineageStatisticsMapperTest {

    @Test
    void mapWithStatistics() throws Exception {
        LineageStatisticsMapper mapper = new LineageStatisticsMapper();
        List<TaxonomyLineage> lineage =
                List.of(
                        new TaxonomyLineageBuilder().taxonId(100L).build(),
                        new TaxonomyLineageBuilder().taxonId(200L).build());
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder().taxonId(9606L).lineagesSet(lineage).build();
        TaxonomyStatistics stat =
                new TaxonomyStatisticsBuilder()
                        .reviewedProteinCount(1L)
                        .unreviewedProteinCount(2L)
                        .proteomeCount(3L)
                        .referenceProteomeCount(4L)
                        .build();

        TaxonomyStatisticsWrapper wrapper =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(stat)
                        .organismReviewedProtein(true)
                        .organismUnreviewedProtein(true)
                        .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(entry, Optional.of(wrapper));

        Iterator<Tuple2<String, TaxonomyStatisticsWrapper>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Tuple2<String, TaxonomyStatisticsWrapper>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(3, resultList.size());

        assertEquals("9606", resultList.get(0)._1);
        assertEquals(wrapper, resultList.get(0)._2);

        assertEquals("100", resultList.get(1)._1);
        TaxonomyStatisticsWrapper lineageWrapper = resultList.get(1)._2;
        assertEquals(stat, lineageWrapper.getStatistics());
        assertFalse(lineageWrapper.isOrganismReviewedProtein());
        assertFalse(lineageWrapper.isOrganismUnreviewedProtein());

        assertEquals("200", resultList.get(2)._1);
        lineageWrapper = resultList.get(2)._2;
        assertEquals(stat, lineageWrapper.getStatistics());
        assertFalse(lineageWrapper.isOrganismReviewedProtein());
        assertFalse(lineageWrapper.isOrganismUnreviewedProtein());
    }

    @Test
    void mapWithEmptyStat() throws Exception {
        LineageStatisticsMapper mapper = new LineageStatisticsMapper();
        List<TaxonomyLineage> lineage = List.of(new TaxonomyLineageBuilder().taxonId(100L).build());
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder().taxonId(9606L).lineagesSet(lineage).build();

        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple =
                new Tuple2<>(entry, Optional.empty());

        Iterator<Tuple2<String, TaxonomyStatisticsWrapper>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Tuple2<String, TaxonomyStatisticsWrapper>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());

        TaxonomyStatisticsWrapper emptyWrapper =
                TaxonomyStatisticsWrapper.builder()
                        .statistics(new TaxonomyStatisticsBuilder().build())
                        .build();
        assertEquals("9606", resultList.get(0)._1);
        assertEquals(emptyWrapper, resultList.get(0)._2);
        assertEquals("100", resultList.get(1)._1);
        assertEquals(emptyWrapper, resultList.get(1)._2);
    }
}
