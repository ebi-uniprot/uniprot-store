package org.uniprot.store.spark.indexer.uniprot.mapper;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.uniprot.UniProtDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2019-11-13
 */
class TaxonomyEntryToUniProtDocumentTest {

    @Test
    void testDocumentWithValidPopularOrganismsWithLineage() throws Exception {
        TaxonomyLineage lineage =
                new TaxonomyLineageBuilder()
                        .taxonId(1111L)
                        .scientificName("lineage scientific 1111")
                        .commonName("lineage common 1111")
                        .synonymsAdd("lineage synonym 1111")
                        .build();

        TaxonomyLineage lineage2 =
                new TaxonomyLineageBuilder()
                        .taxonId(2222L)
                        .scientificName("lineage scientific 2222")
                        .commonName("lineage common 2222")
                        .synonymsAdd("lineage synonym 2222")
                        .build();

        TaxonomyEntry organismEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(9606L)
                        .commonName("organism common name")
                        .scientificName("organism scientific name")
                        .mnemonic("organism mnemonic")
                        .synonymsAdd("organism synonym")
                        .lineagesAdd(lineage)
                        .lineagesAdd(lineage2)
                        .build();

        List<TaxonomyEntry> entries = new ArrayList<>();
        entries.add(organismEntry);

        UniProtDocument doc = new UniProtDocument();
        doc.organismTaxId = 9606;

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertEquals(4, result.organismName.size());
        assertTrue(result.organismName.contains("organism scientific name"));
        assertTrue(result.organismName.contains("organism common name"));
        assertTrue(result.organismName.contains("organism mnemonic"));
        assertTrue(result.organismName.contains("organism synonym"));

        assertEquals("organism scientific name organ", result.organismSort);
        assertEquals("Human", result.popularOrganism);
        assertNull(result.otherOrganism);

        assertEquals(4, result.organismTaxon.size());
        assertTrue(result.organismTaxon.contains("lineage common 1111"));
        assertTrue(result.organismTaxon.contains("lineage scientific 1111"));
        assertTrue(result.organismTaxon.contains("lineage common 2222"));
        assertTrue(result.organismTaxon.contains("lineage scientific 2222"));

        assertEquals(2, result.taxLineageIds.size());
        assertTrue(result.taxLineageIds.contains(1111));
        assertTrue(result.taxLineageIds.contains(2222));

        assertEquals(10, result.content.size());
        assertTrue(result.content.containsAll(result.organismTaxon));
        assertTrue(result.content.containsAll(result.organismName));
        assertTrue(result.content.contains("1111"));
        assertTrue(result.content.contains("2222"));
    }

    @Test
    void testDocumentOtherOrganismWithMnemonic() throws Exception {
        TaxonomyEntry organismEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(1000L)
                        .commonName("organism common name")
                        .scientificName("organism scientific name")
                        .mnemonic("organism mnemonic")
                        .build();

        List<TaxonomyEntry> entries = new ArrayList<>();
        entries.add(organismEntry);

        UniProtDocument doc = new UniProtDocument();
        doc.organismTaxId = 1000;

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals("organism mnemonic", result.otherOrganism);
        assertNull(result.popularOrganism);
    }

    @Test
    void testDocumentOtherOrganismWithCommon() throws Exception {
        TaxonomyEntry organismEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(1000L)
                        .commonName("organism common name")
                        .scientificName("organism scientific name")
                        .synonymsAdd("organism synonym")
                        .build();

        List<TaxonomyEntry> entries = new ArrayList<>();
        entries.add(organismEntry);

        UniProtDocument doc = new UniProtDocument();
        doc.organismTaxId = 1000;

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals("organism common name", result.otherOrganism);
        assertNull(result.popularOrganism);
    }

    @Test
    void testDocumentOtherOrganismWithScientific() throws Exception {
        TaxonomyEntry organismEntry =
                new TaxonomyEntryBuilder()
                        .taxonId(1000L)
                        .scientificName("organism scientific name")
                        .build();

        List<TaxonomyEntry> entries = new ArrayList<>();
        entries.add(organismEntry);

        UniProtDocument doc = new UniProtDocument();
        doc.organismTaxId = 1000;

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);
        assertNotNull(result);

        assertEquals("organism scientific name", result.otherOrganism);
        assertNull(result.popularOrganism);
    }

    @Test
    void testDocumentWithValidOrganismHosts() throws Exception {

        TaxonomyEntry organismHost9606 =
                new TaxonomyEntryBuilder()
                        .taxonId(9606L)
                        .commonName("organism common name 9606")
                        .scientificName("organism scientific name 9606")
                        .mnemonic("organism mnemonic 9606")
                        .synonymsAdd("organism synonym 9606")
                        .build();

        TaxonomyEntry organismHost9000 =
                new TaxonomyEntryBuilder()
                        .taxonId(9000L)
                        .commonName("organism common name 9000")
                        .scientificName("organism scientific name 9000")
                        .mnemonic("organism mnemonic 9000")
                        .synonymsAdd("organism synonym 9000")
                        .build();

        UniProtDocument doc = new UniProtDocument();
        doc.organismHostIds.add(9606);
        doc.organismHostIds.add(9000);

        List<TaxonomyEntry> entries = new ArrayList<>();
        entries.add(organismHost9606);
        entries.add(organismHost9000);

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertEquals(8, result.content.size());
        assertTrue(result.content.containsAll(result.organismHostNames));
        // organism hosts ids is already added to content in entry converter

        assertTrue(result.organismHostNames.contains("organism synonym 9000"));
        assertTrue(result.organismHostNames.contains("organism common name 9000"));
        assertTrue(result.organismHostNames.contains("organism mnemonic 9000"));
        assertTrue(result.organismHostNames.contains("organism scientific name 9000"));

        assertTrue(result.organismHostNames.contains("organism synonym 9606"));
        assertTrue(result.organismHostNames.contains("organism common name 9606"));
        assertTrue(result.organismHostNames.contains("organism mnemonic 9606"));
        assertTrue(result.organismHostNames.contains("organism scientific name 9606"));
    }

    @Test
    void testDocumentWithoutOrganismsAndHosts() throws Exception {
        List<TaxonomyEntry> entries = new ArrayList<>();

        UniProtDocument doc = new UniProtDocument();

        Tuple2<UniProtDocument, Optional<Iterable<TaxonomyEntry>>> tuple =
                new Tuple2<>(doc, Optional.of(entries));
        TaxonomyEntryToUniProtDocument mapper = new TaxonomyEntryToUniProtDocument();

        UniProtDocument result = mapper.call(tuple);

        assertNotNull(result);

        assertEquals(0, result.organismTaxId);
        assertTrue(result.organismName.isEmpty());
        assertNull(result.organismSort);

        assertTrue(result.taxLineageIds.isEmpty());
        assertTrue(result.organismTaxon.isEmpty());

        assertNull(result.popularOrganism);
        assertNull(result.otherOrganism);

        assertTrue(result.content.isEmpty());
    }
}
