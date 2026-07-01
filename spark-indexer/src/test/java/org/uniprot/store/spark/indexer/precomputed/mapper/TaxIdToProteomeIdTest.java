package org.uniprot.store.spark.indexer.precomputed.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.uniprot.core.proteome.ProteomeEntry;
import org.uniprot.core.proteome.impl.ProteomeEntryBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import scala.Tuple2;

class TaxIdToProteomeIdTest {

    private final TaxIdToProteomeId mapper = new TaxIdToProteomeId();

    @Test
    void canMapProteomeEntryToTaxonomyIdAndProteomeId() throws Exception {
        ProteomeEntry proteomeEntry =
                new ProteomeEntryBuilder()
                        .proteomeId("UP000005640")
                        .taxonomy(new TaxonomyBuilder().taxonId(9606L).build())
                        .build();

        Tuple2<Integer, String> result = mapper.call(proteomeEntry);

        assertEquals(9606, result._1);
        assertEquals("UP000005640", result._2);
    }
}
