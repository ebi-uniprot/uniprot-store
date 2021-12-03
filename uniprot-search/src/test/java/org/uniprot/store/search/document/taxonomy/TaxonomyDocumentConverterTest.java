package org.uniprot.store.search.document.taxonomy;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.uniprot.core.json.parser.taxonomy.TaxonomyJsonConfig;
import org.uniprot.core.taxonomy.TaxonomyEntry;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.TaxonomyRank;
import org.uniprot.core.taxonomy.TaxonomyStatistics;
import org.uniprot.core.taxonomy.impl.TaxonomyEntryBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStatisticsBuilder;
import org.uniprot.core.taxonomy.impl.TaxonomyStrainBuilder;
import org.uniprot.core.uniprotkb.taxonomy.impl.TaxonomyBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

class TaxonomyDocumentConverterTest {

    private static final ObjectMapper mapper =
            TaxonomyJsonConfig.getInstance().getFullObjectMapper();

    @Test
    void convertMinimal() throws IOException {
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder()
                        .taxonId(10L)
                        .parent(new TaxonomyBuilder().taxonId(1L).build())
                        .rank(TaxonomyRank.NO_RANK)
                        .build();
        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(mapper);
        TaxonomyDocument result = converter.convert(entry).build();
        assertNotNull(result);
        assertEquals(10L, result.getTaxId());
        assertEquals(TaxonomyRank.NO_RANK.getName(), result.getRank());
        assertNull(result.getParent());

        assertNotNull(result.getTaxonomyObj());
        TaxonomyEntry documentEntry =
                mapper.readValue(result.getTaxonomyObj(), TaxonomyEntry.class);
        assertEquals(entry.getTaxonId(), documentEntry.getTaxonId());
        assertEquals(entry.getRank(), documentEntry.getRank());
        assertNull(documentEntry.getParent());
    }

    @Test
    void convertFullEntry() throws IOException {
        TaxonomyEntry entry = getEntry();
        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(mapper);
        TaxonomyDocument document = converter.convert(entry).build();
        assertNotNull(document);
        assertEquals("9606", document.getId());
        assertEquals(9606L, document.getTaxId());
        assertEquals(9605L, document.getParent());

        assertEquals("kingdom", document.getRank());
        assertEquals("scientificName", document.getScientific());
        assertEquals("commonName", document.getCommon());
        assertEquals("synonym", document.getSynonym());
        assertEquals("mnemonic", document.getMnemonic());
        assertEquals("Lineage Scientific Name", document.getSuperkingdom());

        assertTrue(document.isHidden());
        assertTrue(document.isActive());
        assertTrue(document.isLinked());

        assertNotNull(document.getStrain());
        assertTrue(document.getStrain().contains("strain1 ; strain2"));
        assertTrue(document.getStrain().contains("strain3"));

        assertNotNull(document.getHost());
        assertTrue(document.getHost().contains(9600L));
        assertTrue(document.getHost().contains(9601L));

        assertNotNull(document.getAncestor());
        assertTrue(document.getAncestor().contains(9603L));

        assertNotNull(document.getOtherNames());
        assertTrue(document.getOtherNames().contains("otherName"));
        assertTrue(document.getOtherNames().contains("otherName2"));

        TaxonomyEntry documentEntry =
                mapper.readValue(document.getTaxonomyObj(), TaxonomyEntry.class);
        assertEquals(entry, documentEntry);
    }

    private TaxonomyEntry getEntry() {
        TaxonomyEntryBuilder builder = new TaxonomyEntryBuilder();
        builder.taxonId(9606L);
        builder.scientificName("scientificName");
        builder.commonName("commonName");
        builder.mnemonic("mnemonic");
        builder.parent(new TaxonomyBuilder().taxonId(9605L).build());
        builder.rank(TaxonomyRank.KINGDOM);
        builder.hidden(true);
        builder.active(true);
        builder.statistics(getStatistics());
        builder.synonymsAdd("synonym");
        builder.otherNamesAdd("otherName");
        builder.otherNamesAdd("otherName2");
        builder.lineagesAdd(getTaxonomyLineage());
        builder.strainsAdd(
                new TaxonomyStrainBuilder().name("strain1").synonymsAdd("strain2").build());
        builder.strainsAdd(new TaxonomyStrainBuilder().name("strain3").build());
        builder.hostsAdd(new TaxonomyBuilder().taxonId(9600L).build());
        builder.hostsAdd(new TaxonomyBuilder().taxonId(9601L).build());
        builder.linksAdd("link");

        return builder.build();
    }

    private TaxonomyStatistics getStatistics() {
        return new TaxonomyStatisticsBuilder()
                .reviewedProteinCount(10)
                .unreviewedProteinCount(20)
                .referenceProteomeCount(2)
                .proteomeCount(1)
                .build();
    }

    public static TaxonomyLineage getTaxonomyLineage() {
        TaxonomyLineageBuilder builder = new TaxonomyLineageBuilder();
        builder.taxonId(9603L)
                .scientificName("Lineage Scientific Name")
                .commonName("Lineage Common Name")
                .synonymsAdd("Lineage synonym")
                .hidden(true)
                .rank(TaxonomyRank.SUPERKINGDOM);
        return builder.build();
    }
}
