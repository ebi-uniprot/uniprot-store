package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;

class UniParcJoinUtilsTest {

    private static final TaxonomyEntry entry1 =
            new TaxonomyEntryBuilder().taxonId(1L).scientificName("sn1").commonName("cn1").build();
    private static final TaxonomyEntry entry2 =
            new TaxonomyEntryBuilder().taxonId(2L).scientificName("sn2").commonName("cn2").build();

    @Test
    void validMappedTaxons() {
        Map<Long, TaxonomyEntry> mappedEntries =
                UniParcJoinUtils.getMappedTaxons(List.of(entry1, entry2, entry1));
        assertNotNull(mappedEntries);
        assertEquals(2, mappedEntries.size());
        assertEquals(entry1, mappedEntries.get(1L));
        assertEquals(entry2, mappedEntries.get(2L));
    }

    @Test
    void emptyMappedTaxons() {
        Map<Long, TaxonomyEntry> mappedEntries = UniParcJoinUtils.getMappedTaxons(List.of());
        assertNotNull(mappedEntries);
        assertTrue(mappedEntries.isEmpty());
    }

    @Test
    void canMapTaxonomy() {
        Map<Long, TaxonomyEntry> map = UniParcJoinUtils.getMappedTaxons(List.of(entry1, entry2));
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .organism(new OrganismBuilder().taxonId(2).build())
                        .build();
        UniParcCrossReference result = UniParcJoinUtils.mapTaxonomy(xref, map);
        assertNotNull(result);
        assertNotNull(result.getOrganism());
        assertEquals(entry2.getTaxonId(), result.getOrganism().getTaxonId());
        assertEquals(entry2.getScientificName(), result.getOrganism().getScientificName());
        assertEquals(entry2.getCommonName(), result.getOrganism().getCommonName());
    }

    @Test
    void canNotMapTaxonomy() {
        Map<Long, TaxonomyEntry> map = UniParcJoinUtils.getMappedTaxons(List.of(entry1, entry2));
        UniParcCrossReference xref =
                new UniParcCrossReferenceBuilder()
                        .organism(new OrganismBuilder().taxonId(3).build())
                        .build();
        UniParcCrossReference result = UniParcJoinUtils.mapTaxonomy(xref, map);
        assertNotNull(result);
        assertNotNull(result.getOrganism());
        assertEquals(3L, result.getOrganism().getTaxonId());
        assertEquals("", result.getOrganism().getScientificName());
        assertEquals("", result.getOrganism().getCommonName());
    }
}
