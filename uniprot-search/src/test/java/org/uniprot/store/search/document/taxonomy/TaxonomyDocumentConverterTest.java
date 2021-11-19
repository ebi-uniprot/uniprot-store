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

    private static ObjectMapper mapper = TaxonomyJsonConfig.getInstance().getFullObjectMapper();

    @Test
    void convertMinimal() throws IOException {
        TaxonomyEntry entry =
                new TaxonomyEntryBuilder()
                        .taxonId(10L)
                        .parent(new TaxonomyBuilder().taxonId(1L).build())
                        .rank(TaxonomyRank.NO_RANK)
                        .build();
        TaxonomyDocumentConverter converter = new TaxonomyDocumentConverter(mapper);
        TaxonomyDocument result = converter.convert(entry);
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
        TaxonomyDocument result = converter.convert(entry);
        assertNotNull(result);
        assertEquals("9606", result.getId());
        assertEquals(9606L, result.getTaxId());
        assertEquals(9605L, result.getParent());

        assertEquals("kingdom", result.getRank());
        assertEquals("scientificName", result.getScientific());
        assertEquals("commonName", result.getCommon());
        assertEquals("synonym", result.getSynonym());
        assertEquals("mnemonic", result.getMnemonic());
        assertEquals("Lineage Scientific Name", result.getSuperkingdom());

        assertTrue(result.isHidden());
        assertTrue(result.isActive());
        assertTrue(result.isLinked());

        assertNotNull(result.getTaxonomiesWith());
        assertTrue(result.getTaxonomiesWith().contains("1_uniprotkb"));
        assertTrue(result.getTaxonomiesWith().contains("2_reviewed"));
        assertTrue(result.getTaxonomiesWith().contains("4_reference"));
        assertTrue(result.getTaxonomiesWith().contains("5_proteome"));

        assertNotNull(result.getStrain());
        assertTrue(result.getStrain().contains("strain1 ; strain2"));
        assertTrue(result.getStrain().contains("strain3"));

        assertNotNull(result.getHost());
        assertTrue(result.getHost().contains(9600L));
        assertTrue(result.getHost().contains(9601L));

        assertNotNull(result.getAncestor());
        assertTrue(result.getAncestor().contains(9603L));

        assertNotNull(result.getOtherNames());
        assertTrue(result.getOtherNames().contains("otherName"));
        assertTrue(result.getOtherNames().contains("otherName2"));

        TaxonomyEntry documentEntry =
                mapper.readValue(result.getTaxonomyObj(), TaxonomyEntry.class);
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
