package org.uniprot.store.spark.indexer.uniref.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.uniref.UniRefDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-02-10
 */
class UniRefTaxonomyJoinTest {

    @Test
    void testWithLineage() throws Exception {
        UniRefDocument doc = UniRefDocument.builder().id("test").build();

        List<TaxonomyLineage> lineage = new ArrayList<>();
        lineage.add(
                new TaxonomyLineageBuilder()
                        .taxonId(1L)
                        .commonName("commonName1")
                        .scientificName("scientificName1")
                        .build());

        lineage.add(
                new TaxonomyLineageBuilder()
                        .taxonId(2L)
                        .commonName("commonName2")
                        .scientificName("scientificName2")
                        .build());

        TaxonomyEntry entry = new TaxonomyEntryBuilder().lineagesSet(lineage).build();
        Optional<TaxonomyEntry> tax = Optional.of(entry);
        Tuple2<UniRefDocument, Optional<TaxonomyEntry>> tuple = new Tuple2<>(doc, tax);

        UniRefTaxonomyJoin mapper = new UniRefTaxonomyJoin();
        UniRefDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(2, result.getTaxLineageIds().size());
        assertTrue(result.getTaxLineageIds().contains(1));
        assertTrue(result.getTaxLineageIds().contains(2));

        assertEquals(4, result.getOrganismTaxons().size());
        assertTrue(result.getOrganismTaxons().contains("commonName1"));
        assertTrue(result.getOrganismTaxons().contains("scientificName1"));
        assertTrue(result.getOrganismTaxons().contains("commonName2"));
        assertTrue(result.getOrganismTaxons().contains("scientificName2"));
    }

    @Test
    void testWithoutLineage() throws Exception {
        UniRefDocument doc = UniRefDocument.builder().id("test").build();

        Optional<TaxonomyEntry> tax = Optional.empty();
        Tuple2<UniRefDocument, Optional<TaxonomyEntry>> tuple = new Tuple2<>(doc, tax);

        UniRefTaxonomyJoin mapper = new UniRefTaxonomyJoin();
        UniRefDocument result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(doc, result);
    }
}
