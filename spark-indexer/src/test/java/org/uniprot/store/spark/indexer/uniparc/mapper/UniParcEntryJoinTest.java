package org.uniprot.store.spark.indexer.uniparc.mapper;

import static org.junit.jupiter.api.Assertions.*;
import static org.uniprot.core.uniparc.UniParcCrossReference.PROPERTY_SOURCES;

import java.util.*;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.Property;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.uniparc.UniParcCrossReference;
import org.uniprot.core.uniparc.UniParcDatabase;
import org.uniprot.core.uniparc.UniParcEntry;
import org.uniprot.core.uniparc.impl.ProteomeBuilder;
import org.uniprot.core.uniparc.impl.UniParcCrossReferenceBuilder;
import org.uniprot.core.uniparc.impl.UniParcEntryBuilder;
import org.uniprot.core.uniparc.impl.UniParcIdBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.store.spark.indexer.uniparc.model.UniParcTaxonomySequenceSource;

import scala.Tuple2;

class UniParcEntryJoinTest {

    @Test
    void validTaxonomyJoin() throws Exception {
        List<TaxonomyEntry> taxonomyEntries = getTaxonomyEntries();

        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(null, taxonomyEntries);
        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(tuple);
        assertNotNull(result);
        assertNotNull(result.getUniParcCrossReferences());
        List<UniParcCrossReference> xrefs = result.getUniParcCrossReferences();
        assertEquals(4, xrefs.size());

        UniParcCrossReference xref = xrefs.get(0);
        assertNotNull(xref.getOrganism());
        assertEquals(10L, xref.getOrganism().getTaxonId());
        assertEquals("sName10", xref.getOrganism().getScientificName());
        assertEquals("cName10", xref.getOrganism().getCommonName());

        xref = xrefs.get(1);
        assertNotNull(xref.getOrganism());
        assertEquals(11L, xref.getOrganism().getTaxonId());
        assertEquals("sName11", xref.getOrganism().getScientificName());
        assertEquals("cName11", xref.getOrganism().getCommonName());

        xref = xrefs.get(2);
        assertNull(xref.getOrganism());
    }

    @Test
    void notFoundTaxonomyReturnEntryWithoutAnyChange() throws Exception {
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();

        UniParcEntry entry = getUniParcEntry();
        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(null, taxonomyEntries);
        UniParcEntryJoin mapper = new UniParcEntryJoin();

        UniParcEntry result = mapper.call(tuple);
        assertEquals(entry, result);
    }

    @Test
    void validSequenceSourceMultipleSourcesJoin() throws Exception {
        Map<String, Set<String>> sequenceSource = new HashMap<>();
        sequenceSource.put("P12345", Set.of("AC12345", "AC54321"));

        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(sequenceSource, null);
        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(4, result.getUniParcCrossReferences().size());
        UniParcCrossReference uniProtXref = result.getUniParcCrossReferences().get(0);
        assertNotNull(uniProtXref);
        assertEquals("P12345", uniProtXref.getId());
        assertEquals(2, uniProtXref.getProperties().size());
        Property sourceProperty = uniProtXref.getProperties().get(0);
        validateSourceProperty(sourceProperty, "EMBL:AC54321:UP000000001:Chromosome");
        sourceProperty = uniProtXref.getProperties().get(1);
        validateSourceProperty(sourceProperty, "EMBL:AC12345:UP000005640:Chromosome");
    }

    @Test
    void notFoundSequenceSourceReturnEntryWithoutAddedProperty() throws Exception {
        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(new HashMap<>(), null);
        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(4, result.getUniParcCrossReferences().size());
        UniParcCrossReference uniProtXref = result.getUniParcCrossReferences().get(0);
        assertNotNull(uniProtXref);
        assertEquals("P12345", uniProtXref.getId());
        assertEquals(0, uniProtXref.getProperties().size());
    }

    @Test
    void validSequenceSourceOnlyMapSourcesWithProteomeJoin() throws Exception {
        Map<String, Set<String>> sequenceSource = new HashMap<>();
        sequenceSource.put("P12345", Set.of("AC12345", "NO_PROTEOME"));

        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(sequenceSource, null);
        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(4, result.getUniParcCrossReferences().size());
        UniParcCrossReference uniProtXref = result.getUniParcCrossReferences().get(0);
        assertNotNull(uniProtXref);
        assertEquals("P12345", uniProtXref.getId());
        assertEquals(2, uniProtXref.getProperties().size());
        Property sourceProperty = uniProtXref.getProperties().get(0);
        validateSourceProperty(sourceProperty, "EMBL:AC12345:UP000005640:Chromosome");
    }

    @Test
    void validSequenceSourceAndTaxonomyJoin() throws Exception {
        Map<String, Set<String>> sequenceSource = new HashMap<>();
        sequenceSource.put("P12345", Set.of("AC12345"));
        List<TaxonomyEntry> taxonomyEntries = getTaxonomyEntries();

        Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>> tuple =
                prepareTuple(sequenceSource, taxonomyEntries);
        UniParcEntryJoin mapper = new UniParcEntryJoin();
        UniParcEntry result = mapper.call(tuple);

        assertNotNull(result);
        assertEquals(4, result.getUniParcCrossReferences().size());
        UniParcCrossReference uniProtXref = result.getUniParcCrossReferences().get(0);
        assertNotNull(uniProtXref);
        assertEquals("P12345", uniProtXref.getId());
        assertEquals(1, uniProtXref.getProperties().size());
        Property sourceProperty = uniProtXref.getProperties().get(0);
        validateSourceProperty(sourceProperty, "EMBL:AC12345:UP000005640:Chromosome");

        assertNotNull(uniProtXref.getOrganism());
        assertEquals(10L, uniProtXref.getOrganism().getTaxonId());
        assertEquals("sName10", uniProtXref.getOrganism().getScientificName());
        assertEquals("cName10", uniProtXref.getOrganism().getCommonName());
    }

    private static void validateSourceProperty(Property sourceProperty, String expectedValue) {
        assertEquals(PROPERTY_SOURCES, sourceProperty.getKey());
        assertEquals(expectedValue, sourceProperty.getValue());
    }

    private Tuple2<String, Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>>>
            prepareTuple(
                    Map<String, Set<String>> sequenceSource, List<TaxonomyEntry> taxonomyEntries) {
        UniParcEntry entry = getUniParcEntry();

        Tuple2<UniParcEntry, Optional<UniParcTaxonomySequenceSource>> innerTuple =
                new Tuple2<>(
                        entry,
                        Optional.of(
                                new UniParcTaxonomySequenceSource(
                                        taxonomyEntries, sequenceSource)));
        return new Tuple2<>("UP000000200", innerTuple);
    }

    private static UniParcEntry getUniParcEntry() {
        return new UniParcEntryBuilder()
                .uniParcId(new UniParcIdBuilder("UP000000200").build())
                .uniParcCrossReferencesAdd(
                        new UniParcCrossReferenceBuilder()
                                .id("P12345")
                                .database(UniParcDatabase.TREMBL)
                                .organism(new OrganismBuilder().taxonId(10).build())
                                .build())
                .uniParcCrossReferencesAdd(
                        new UniParcCrossReferenceBuilder()
                                .id("AC12345")
                                .database(UniParcDatabase.EMBL)
                                .organism(new OrganismBuilder().taxonId(11).build())
                                .proteomesAdd(
                                        new ProteomeBuilder()
                                                .id("UP000005640")
                                                .component("Chromosome")
                                                .build())
                                .build())
                .uniParcCrossReferencesAdd(
                        new UniParcCrossReferenceBuilder()
                                .id("AC54321")
                                .database(UniParcDatabase.EMBL)
                                .proteomesAdd(
                                        new ProteomeBuilder()
                                                .id("UP000000001")
                                                .component("Chromosome")
                                                .build())
                                .build())
                .uniParcCrossReferencesAdd(
                        new UniParcCrossReferenceBuilder()
                                .id("NO_PROTEOME")
                                .database(UniParcDatabase.REFSEQ)
                                .build())
                .build();
    }

    private List<TaxonomyEntry> getTaxonomyEntries() {
        List<TaxonomyEntry> taxonomyEntries = new ArrayList<>();
        TaxonomyEntry entry1 =
                new TaxonomyEntryBuilder()
                        .taxonId(10)
                        .scientificName("sName10")
                        .commonName("cName10")
                        .build();
        taxonomyEntries.add(entry1);

        TaxonomyEntry entry2 =
                new TaxonomyEntryBuilder()
                        .taxonId(11)
                        .scientificName("sName11")
                        .commonName("cName11")
                        .build();
        taxonomyEntries.add(entry2);
        return taxonomyEntries;
    }
}
