package org.uniprot.store.search.document.genecentric;

import static org.junit.jupiter.api.Assertions.*;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.core.json.parser.genecentric.GeneCentricJsonConfig;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author lgonzales
 * @since 10/11/2020
 */
class GeneCentricDocumentConverterTest {

    private static GeneCentricDocumentConverter converter;

    @BeforeAll
    static void setup() {
        ObjectMapper mapper = GeneCentricJsonConfig.getInstance().getFullObjectMapper();
        converter = new GeneCentricDocumentConverter(mapper);
    }

    @Test
    void canConvertGeneCentricEntryToDocument() throws Exception {
        GeneCentricEntry entry = createGeneCentricEntry();
        GeneCentricDocument result = converter.convert(entry);
        assertNotNull(result);
        assertEquals("P12345", result.getAccession());

        assertEquals(2, result.getAccessions().size());
        assertTrue(result.getAccessions().contains("P12345"));
        assertTrue(result.getAccessions().contains("P54321"));

        assertTrue(result.getReviewed());

        assertEquals(2, result.getGeneNames().size());
        assertTrue(result.getGeneNames().contains("canonical Gene"));
        assertTrue(result.getGeneNames().contains("related Gene"));

        assertEquals("UP000000554", result.getUpid());

        assertEquals(9606, result.getOrganismTaxId());

        assertNotNull(result.getGeneCentricStored());
        assertTrue(result.getGeneCentricStored().length > 0);
    }

    @Test
    void canConvertGeneCentricEntryWithoutRelatedToDocument() throws Exception {
        GeneCentricEntry entry =
                new GeneCentricEntryBuilder().canonicalProtein(createCanonicalProtein()).build();
        GeneCentricDocument result = converter.convert(entry);
        assertNotNull(result);
        assertEquals("P12345", result.getAccession());

        assertEquals(1, result.getAccessions().size());
        assertTrue(result.getAccessions().contains("P12345"));

        assertTrue(result.getReviewed());

        assertEquals(1, result.getGeneNames().size());
        assertTrue(result.getGeneNames().contains("canonical Gene"));

        assertNull(result.getUpid());

        assertEquals(9606, result.getOrganismTaxId());

        assertNotNull(result.getGeneCentricStored());
        assertTrue(result.getGeneCentricStored().length > 0);
    }

    @Test
    void canConvertStoredEntryDocument() throws Exception {
        GeneCentricEntry entry = createGeneCentricEntry();
        GeneCentricDocument result = converter.convert(entry);

        assertNotNull(result.getGeneCentricStored());
        assertTrue(result.getGeneCentricStored().length > 0);

        GeneCentricEntry converted = converter.getCanonicalEntryFromDocument(result);
        assertNotNull(converted);

        assertEquals(entry, converted);
    }

    @NotNull
    private GeneCentricEntry createGeneCentricEntry() {
        Protein canonicalProtein = createCanonicalProtein();
        Protein relatedProtein = new ProteinBuilder().id("P54321").geneName("related Gene").build();

        return new GeneCentricEntryBuilder()
                .proteomeId("UP000000554")
                .canonicalProtein(canonicalProtein)
                .relatedProteinsAdd(relatedProtein)
                .build();
    }

    @NotNull
    private Protein createCanonicalProtein() {
        return new ProteinBuilder()
                .entryType(UniProtKBEntryType.SWISSPROT)
                .id("P12345")
                .geneName("canonical Gene")
                .organism(new OrganismBuilder().taxonId(9606L).build())
                .build();
    }
}
