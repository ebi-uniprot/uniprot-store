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
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple =
                new Tuple2<>(entry, Optional.of(stat));

        Iterator<Tuple2<String, TaxonomyStatistics>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Tuple2<String, TaxonomyStatistics>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(3, resultList.size());

        assertEquals("9606", resultList.get(0)._1);
        assertEquals(stat, resultList.get(0)._2);
        assertEquals("100", resultList.get(1)._1);
        assertEquals(stat, resultList.get(1)._2);
        assertEquals("200", resultList.get(2)._1);
        assertEquals(stat, resultList.get(2)._2);
    }

    @Test
    void mapWithEmptyStat() throws Exception {
        LineageStatisticsMapper mapper = new LineageStatisticsMapper();
        List<TaxonomyLineage> lineage = List.of(new TaxonomyLineageBuilder().taxonId(100L).build());
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder().taxonId(9606L).lineagesSet(lineage).build();

        Tuple2<TaxonomyEntry, Optional<TaxonomyStatistics>> tuple =
                new Tuple2<>(entry, Optional.empty());

        Iterator<Tuple2<String, TaxonomyStatistics>> result = mapper.call(tuple);
        assertNotNull(result);
        List<Tuple2<String, TaxonomyStatistics>> resultList = new ArrayList<>();
        result.forEachRemaining(resultList::add);
        assertNotNull(resultList);
        assertEquals(2, resultList.size());

        TaxonomyStatistics emptyStat = new TaxonomyStatisticsBuilder().build();
        assertEquals("9606", resultList.get(0)._1);
        assertEquals(emptyStat, resultList.get(0)._2);
        assertEquals("100", resultList.get(1)._1);
        assertEquals(emptyStat, resultList.get(1)._2);
    }
}
