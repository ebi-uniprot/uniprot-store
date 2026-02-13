package org.uniprot.store.spark.indexer.proteome.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.ProteomeType;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;
import org.uniprot.store.spark.indexer.taxonomy.mapper.model.TaxonomyStatisticsWrapper;

import scala.Tuple2;

class ProteomeTaxonomyStatisticsMapperTest {

    @Test
    void mapReferenceProteome() throws Exception {
        ProteomeTaxonomyStatisticsMapper mapper = new ProteomeTaxonomyStatisticsMapper();
        ProteomeEntry entry =
                new ProteomeEntryBuilder()
                        .proteomeId("UP000000001")
                        .proteomeType(ProteomeType.REFERENCE)
                        .taxonomy(new TaxonomyBuilder().taxonId(100L).build())
                        .build();
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("100", result._1);
        assertNotNull(result._2.getStatistics());
        TaxonomyStatistics statistics = result._2.getStatistics();
        assertEquals(1, statistics.getReferenceProteomeCount());
        assertEquals(1, statistics.getProteomeCount());
    }

    @Test
    void mapNonReferenceProteome() throws Exception {
        ProteomeTaxonomyStatisticsMapper mapper = new ProteomeTaxonomyStatisticsMapper();
        ProteomeEntry entry =
                new ProteomeEntryBuilder()
                        .proteomeId("UP000000001")
                        .proteomeType(ProteomeType.NON_REFERENCE)
                        .taxonomy(new TaxonomyBuilder().taxonId(100L).build())
                        .build();
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("100", result._1);
        assertNotNull(result._2.getStatistics());
        TaxonomyStatistics statistics = result._2.getStatistics();
        assertEquals(0, statistics.getReferenceProteomeCount());
        assertEquals(1, statistics.getProteomeCount());
    }

    @Test
    void mapNonExcludedProteome() throws Exception {
        ProteomeTaxonomyStatisticsMapper mapper = new ProteomeTaxonomyStatisticsMapper();
        ProteomeEntry entry =
                new ProteomeEntryBuilder()
                        .proteomeId("UP000000001")
                        .proteomeType(ProteomeType.EXCLUDED)
                        .taxonomy(new TaxonomyBuilder().taxonId(100L).build())
                        .build();
        Tuple2<String, TaxonomyStatisticsWrapper> result = mapper.call(entry);
        assertNotNull(result);
        assertEquals("100", result._1);
        assertNotNull(result._2.getStatistics());
        TaxonomyStatistics statistics = result._2.getStatistics();
        assertEquals(0, statistics.getReferenceProteomeCount());
        assertEquals(1, statistics.getProteomeCount());
    }
}
