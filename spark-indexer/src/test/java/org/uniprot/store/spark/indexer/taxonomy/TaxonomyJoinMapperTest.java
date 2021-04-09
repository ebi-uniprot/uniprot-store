package org.uniprot.store.spark.indexer.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-14
 */
class TaxonomyJoinMapperTest {

    @Test
    void testMapTaxonomyLineage() throws Exception {
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder()
                        .taxonId(9606L)
                        .parent(new TaxonomyBuilder().taxonId(2L).build())
                        .build();
        List<TaxonomyLineage> lineage = new ArrayList<>();
        lineage.add(
                new TaxonomyLineageBuilder()
                        .taxonId(1L)
                        .commonName("common1")
                        .scientificName("name1")
                        .build());
        lineage.add(
                new TaxonomyLineageBuilder()
                        .taxonId(2L)
                        .commonName("common2")
                        .scientificName("name2")
                        .build());
        Tuple2<TaxonomyEntry, List<TaxonomyLineage>> tuple = new Tuple2<>(entry, lineage);

        TaxonomyJoinMapper mapper = new TaxonomyJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertTrue(result.hasLineage());
        assertEquals(lineage, result.getLineages());

        assertNotNull(result.getParent());
        assertEquals(2L, result.getParent().getTaxonId());
        assertEquals("name2", result.getParent().getScientificName());
        assertEquals("common2", result.getParent().getCommonName());
    }

    @Test
    void testMapTaxonomyEmptyLineage() throws Exception {
        TaxonomyEntry entry = new TaxonomyEntryBuilder().taxonId(9606L).build();
        Tuple2<TaxonomyEntry, List<TaxonomyLineage>> tuple =
                new Tuple2<>(entry, Collections.emptyList());

        TaxonomyJoinMapper mapper = new TaxonomyJoinMapper();
        TaxonomyEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertNotNull(result.getLineages());
        assertFalse(result.hasLineage());
    }
}
