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
import org.uniprot.store.spark.indexer.common.exception.IndexDataStoreException;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 21/01/2021
 */
class UniParcEntryTaxonomyJoinTest {

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
                                        .taxonomy(new OrganismBuilder().taxonId(10).build())
                                        .build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder()
                                        .id("2")
                                        .taxonomy(new OrganismBuilder().taxonId(11).build())
                                        .build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder().id("3").build())
                        .build();
        Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>> innerTuple =
                new Tuple2<>(entry, Optional.of(taxonomyEntries));

        UniParcEntryTaxonomyJoin mapper = new UniParcEntryTaxonomyJoin();
        UniParcEntry result = mapper.call(new Tuple2<>("UP000000001", innerTuple));
        assertNotNull(result);
        assertNotNull(result.getUniParcCrossReferences());
        List<UniParcCrossReference> xrefs = result.getUniParcCrossReferences();
        assertEquals(3, xrefs.size());

        UniParcCrossReference xref = xrefs.get(0);
        assertNotNull(xref.getTaxonomy());
        assertEquals(10L, xref.getTaxonomy().getTaxonId());
        assertEquals("scientificName10", xref.getTaxonomy().getScientificName());
        assertEquals("commonName10", xref.getTaxonomy().getCommonName());

        xref = xrefs.get(1);
        assertNotNull(xref.getTaxonomy());
        assertEquals(11L, xref.getTaxonomy().getTaxonId());
        assertEquals("scientificName11", xref.getTaxonomy().getScientificName());
        assertEquals("commonName11", xref.getTaxonomy().getCommonName());

        xref = xrefs.get(2);
        assertNull(xref.getTaxonomy());
    }

    @Test
    void invalidJoin() throws Exception {
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();

        UniParcEntry entry =
                new UniParcEntryBuilder()
                        .uniParcId(new UniParcIdBuilder("UP000000001").build())
                        .uniParcCrossReferencesAdd(
                                new UniParcCrossReferenceBuilder()
                                        .id("1")
                                        .taxonomy(new OrganismBuilder().taxonId(10).build())
                                        .build())
                        .build();
        Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>> innerTuple =
                new Tuple2<>(entry, Optional.of(taxonomyEntries));

        UniParcEntryTaxonomyJoin mapper = new UniParcEntryTaxonomyJoin();
        Tuple2<String, Tuple2<UniParcEntry, Optional<Iterable<TaxonomyEntry>>>> tuple =
                new Tuple2<>("UP000000001", innerTuple);
        assertThrows(IndexDataStoreException.class, () -> mapper.call(tuple));
    }
}
