package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/01/2021
 */
class UniParcEntryTaxonomyJoinTest {
/*
    @Test
    void validJoin() throws Exception {
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();
        TaxonomyEntry entry1 =
                new TaxonomyEntryBuilder()
                        .taxonId(10)
                        .scientificName("scientificName10")
                        .commonName("commonName10")
                        .build();
        taxonomyEntries.add(entry1);

        TaxonomyEntry entry2 =
                new TaxonomyEntryBuilder()
                        .taxonId(11)
                        .scientificName("scientificName11")
                        .commonName("commonName11")
                        .build();
        taxonomyEntries.add(entry2);

        UniParcEntry entry =
                new UniParcEntryBuilder()
                        .uniParcId(new UniParcIdBuilder("UP000000001").build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder()
                                        .id("1")
                                        .organism(new OrganismBuilder().taxonId(10).build())
                                        .build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder()
                                        .id("2")
                                        .organism(new OrganismBuilder().taxonId(11).build())
                                        .build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder().id("3").build())
                        .build();
        Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>> innerTuple =
                new Tuple2<>(entry, Optional.of(taxonomyEntries));

        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(new Tuple2<>("UP000000001", innerTuple));
        assertNotNull(result);
        assertNotNull(result.getUniParcCrossReferences());
        List<UniParcCrossReference> xrefs = result.getUniParcCrossReferences();
        assertEquals(3, xrefs.size());

        UniParcCrossReference xref = xrefs.get(0);
        assertNotNull(xref.getOrganism());
        assertEquals(10L, xref.getOrganism().getTaxonId());
        assertEquals("scientificName10", xref.getOrganism().getScientificName());
        assertEquals("commonName10", xref.getOrganism().getCommonName());

        xref = xrefs.get(1);
        assertNotNull(xref.getOrganism());
        assertEquals(11L, xref.getOrganism().getTaxonId());
        assertEquals("scientificName11", xref.getOrganism().getScientificName());
        assertEquals("commonName11", xref.getOrganism().getCommonName());

        xref = xrefs.get(2);
        assertNull(xref.getOrganism());
    }

    @Test
    void notFoundTaxonomyReturnEntryWithoutAnyChange() throws Exception {
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();

        UniParcEntry entry =
                new UniParcEntryBuilder()
                        .uniParcId(new UniParcIdBuilder("UP000000001").build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder()
                                        .id("1")
                                        .organism(new OrganismBuilder().taxonId(10).build())
                                        .build())
                        .build();
        Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>> innerTuple =
                new Tuple2<>(entry, Optional.of(taxonomyEntries));

        UniParcEntryJoin mapper = new UniParcEntryJoin();
        Tuple2<String, Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>>> tuple =
                new Tuple2<>("UP000000001", innerTuple);
        UniParcEntry result = mapper.call(tuple);
        assertEquals(entry, result);
    }
    */
}
