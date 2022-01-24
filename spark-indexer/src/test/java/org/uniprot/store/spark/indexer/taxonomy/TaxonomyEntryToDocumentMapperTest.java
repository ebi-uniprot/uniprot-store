package org.uniprot.store.spark.indexer.taxonomy;

import org.apache.spark.api.java.Optional;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.core.uniprotkb.taxonomy.Taxonomy;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.search.document.taxonomy.TaxonomyDocument;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;
import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.*;

class TaxonomyEntryToDocumentMapperTest {

    @Test
    void canMapOrganismWithReviewedUniprotKbEntries() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();
        TaxonomyEntry entry = getTaxonomyEntry();
        TaxonomyStatistics stats = new TaxonomyStatisticsBuilder()
                .reviewedProteinCount(10L)
                .build();
        TaxonomyStatisticsWrapper wrapper = TaxonomyStatisticsWrapper.builder()
                .organismReviewedProtein(true)
                .statistics(stats)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.of(wrapper));

        TaxonomyDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(9606L, result.getTaxId());
        assertNotNull(result.getTaxonomiesWith());
        assertEquals(2, result.getTaxonomiesWith().size());
        assertTrue(result.getTaxonomiesWith().contains("1_uniprotkb"));
        assertTrue(result.getTaxonomiesWith().contains("2_reviewed"));
    }

    @Test
    void canMapOrganismWithUnreviewedUniprotKbEntries() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();

        TaxonomyEntry entry = getTaxonomyEntry();
        TaxonomyStatistics stats = new TaxonomyStatisticsBuilder()
                .unreviewedProteinCount(10L)
                .build();
        TaxonomyStatisticsWrapper wrapper = TaxonomyStatisticsWrapper.builder()
                .organismUnreviewedProtein(true)
                .statistics(stats)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.of(wrapper));

        TaxonomyDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(9606L, result.getTaxId());
        assertNotNull(result.getTaxonomiesWith());
        assertEquals(2, result.getTaxonomiesWith().size());
        assertTrue(result.getTaxonomiesWith().contains("1_uniprotkb"));
        assertTrue(result.getTaxonomiesWith().contains("3_unreviewed"));
    }

    @Test
    void canMapOrganismWithReferenceProteome() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();

        TaxonomyEntry entry = getTaxonomyEntry();
        TaxonomyStatistics stats = new TaxonomyStatisticsBuilder()
                .referenceProteomeCount(10)
                .build();
        TaxonomyStatisticsWrapper wrapper = TaxonomyStatisticsWrapper.builder()
                .statistics(stats)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.of(wrapper));

        TaxonomyDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(9606L, result.getTaxId());
        assertNotNull(result.getTaxonomiesWith());
        assertEquals(2, result.getTaxonomiesWith().size());
        assertTrue(result.getTaxonomiesWith().contains("4_reference"));
        assertTrue(result.getTaxonomiesWith().contains("5_proteome"));
    }

    @Test
    void canMapOrganismWithProteome() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();
        TaxonomyEntry entry = getTaxonomyEntry();
        TaxonomyStatistics stats = new TaxonomyStatisticsBuilder()
                .proteomeCount(10)
                .build();
        TaxonomyStatisticsWrapper wrapper = TaxonomyStatisticsWrapper.builder()
                .statistics(stats)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.of(wrapper));

        TaxonomyDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(9606L, result.getTaxId());
        assertNotNull(result.getTaxonomiesWith());
        assertEquals(1, result.getTaxonomiesWith().size());
        assertTrue(result.getTaxonomiesWith().contains("5_proteome"));
    }

    @Test
    void mapWithAllTaxonomiesWith() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();
        TaxonomyEntry entry = getTaxonomyEntry();
        TaxonomyStatistics stats = new TaxonomyStatisticsBuilder()
                .proteomeCount(10)
                .referenceProteomeCount(20)
                .build();
        TaxonomyStatisticsWrapper wrapper = TaxonomyStatisticsWrapper.builder()
                .organismReviewedProtein(true)
                .organismUnreviewedProtein(true)
                .statistics(stats)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.of(wrapper));

        TaxonomyDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(9606L, result.getTaxId());
        assertNotNull(result.getTaxonomiesWith());
        assertEquals(5, result.getTaxonomiesWith().size());
        assertTrue(result.getTaxonomiesWith().contains("1_uniprotkb"));
        assertTrue(result.getTaxonomiesWith().contains("2_reviewed"));
        assertTrue(result.getTaxonomiesWith().contains("3_unreviewed"));
        assertTrue(result.getTaxonomiesWith().contains("4_reference"));
        assertTrue(result.getTaxonomiesWith().contains("5_proteome"));
    }

    @Test
    void mapWhenRootReturnNull() throws Exception {
        TaxonomyEntryToDocumentMapper mapper = new TaxonomyEntryToDocumentMapper();

        TaxonomyEntry entry = new TaxonomyEntryBuilder()
                .taxonId(1L)
                .build();
        Tuple2<TaxonomyEntry, Optional<TaxonomyStatisticsWrapper>> tuple = new Tuple2<>(entry,Optional.empty());
        TaxonomyDocument result = mapper.call(tuple);
        assertNull(result);
    }

    @NotNull
    private TaxonomyEntry getTaxonomyEntry() {
        Taxonomy parent = new TaxonomyBuilder()
                .taxonId(10L)
                .build();
        return new TaxonomyEntryBuilder()
                .taxonId(9606L)
                .parent(parent)
                .rank(TaxonomyRank.FAMILY)
                .build();
    }
}