package org.uniprot.store.spark.indexer.genecentric.mapper;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.uniprot.core.genecentric.GeneCentricEntry;
import org.uniprot.core.genecentric.Protein;
import org.uniprot.core.genecentric.impl.GeneCentricEntryBuilder;
import org.uniprot.core.genecentric.impl.ProteinBuilder;
import org.uniprot.core.uniprotkb.UniProtKBEntryType;
import org.uniprot.core.uniprotkb.taxonomy.impl.OrganismBuilder;
import org.uniprot.store.search.document.proteome.GeneCentricDocument;

/**
 * @author lgonzales
 * @since 23/10/2020
 */
class GeneCentricToDocumentTest {

    @Test
    void canMapEntryToDocument() throws Exception {
        GeneCentricToDocument mapper = new GeneCentricToDocument();
        Protein canonicalProtein =
                new ProteinBuilder()
                        .entryType(UniProtKBEntryType.SWISSPROT)
                        .id("P12345")
                        .geneName("canonical Gene")
                        .organism(new OrganismBuilder().taxonId(9606L).build())
                        .build();
        Protein relatedProtein = new ProteinBuilder().id("P54321").geneName("related Gene").build();

        GeneCentricEntry entry =
                new GeneCentricEntryBuilder()
                        .proteomeId("UP000000554")
                        .canonicalProtein(canonicalProtein)
                        .relatedProteinsAdd(relatedProtein)
                        .build();
        GeneCentricDocument result = mapper.call(entry);
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
}
