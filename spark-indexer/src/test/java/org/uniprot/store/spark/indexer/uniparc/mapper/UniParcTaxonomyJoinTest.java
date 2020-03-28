package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.uniparc.UniParcDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-02-20
 */
class UniParcTaxonomyJoinTest {

    @Test
    void testJoinTaxonomiesComplete() throws Exception {
        UniParcDocument doc = new UniParcDocument();
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();
        TaxonomyEntry entry1 =
                new TaxonomyEntryBuilder()
                        .taxonId(10)
                        .lineagesAdd(
                                new TaxonomyLineageBuilder()
                                        .taxonId(101)
                                        .scientificName("scientificName101")
                                        .commonName("commonName101")
                                        .build())
                        .lineagesAdd(
                                new TaxonomyLineageBuilder()
                                        .taxonId(102)
                                        .scientificName("scientificName102")
                                        .build())
                        .build();
        taxonomyEntries.add(entry1);

        TaxonomyEntry entry2 =
                new TaxonomyEntryBuilder()
                        .taxonId(11)
                        .lineagesAdd(
                                new TaxonomyLineageBuilder()
                                        .taxonId(111)
                                        .scientificName("scientificName111")
                                        .commonName("commonName111")
                                        .build())
                        .build();
        taxonomyEntries.add(entry2);

        Tuple2<UniParcDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(taxonomyEntries));

        UniParcTaxonomyJoin mapper = new UniParcTaxonomyJoin();
        UniParcDocument result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getOrganismTaxons());
        assertEquals(5, result.getOrganismTaxons().size());
        assertTrue(result.getOrganismTaxons().contains("scientificName101"));
        assertTrue(result.getOrganismTaxons().contains("commonName101"));
        assertTrue(result.getOrganismTaxons().contains("scientificName102"));
        assertTrue(result.getOrganismTaxons().contains("scientificName111"));
        assertTrue(result.getOrganismTaxons().contains("commonName111"));

        assertEquals(3, result.getTaxLineageIds().size());
        assertTrue(result.getTaxLineageIds().contains(101));
        assertTrue(result.getTaxLineageIds().contains(102));
        assertTrue(result.getTaxLineageIds().contains(111));
    }

    @Test
    void testJoinTaxonomiesEmptyList() throws Exception {
        UniParcDocument doc = new UniParcDocument();
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();
        Tuple2<UniParcDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(taxonomyEntries));

        UniParcTaxonomyJoin mapper = new UniParcTaxonomyJoin();
        UniParcDocument result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getOrganismTaxons());
        assertTrue(result.getOrganismTaxons().isEmpty());
    }

    @Test
    void testJoinTaxonomiesEmptyOptional() throws Exception {
        UniParcDocument doc = new UniParcDocument();
        Tuple2<UniParcDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.empty());

        UniParcTaxonomyJoin mapper = new UniParcTaxonomyJoin();
        UniParcDocument result = mapper.call(tuple);
        assertNotNull(result);
        assertNull(result.getOrganismTaxons());
    }
}
