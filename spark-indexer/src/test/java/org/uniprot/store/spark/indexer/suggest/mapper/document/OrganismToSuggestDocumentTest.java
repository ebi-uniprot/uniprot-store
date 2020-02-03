package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.builder.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author lgonzales
 * @since 2020-01-21
 */
class OrganismToSuggestDocumentTest {

    @Test
    void testOrganismToSuggestDocument() throws Exception {
        TaxonomyLineage organism =
                new TaxonomyLineageBuilder().taxonId(1111).scientificName("value").build();
        OrganismToSuggestDocument mapper = new OrganismToSuggestDocument("test1");

        SuggestDocument result =
                mapper.call(new Tuple2<>("1111", Collections.singletonList(organism)));
        assertNotNull(result);

        assertEquals("test1", result.dictionary);
        assertEquals("1111", result.id);
        assertEquals("value", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testGetOrganismSuggestDocumentWithoutAltValues() {
        TaxonomyLineage organism =
                new TaxonomyLineageBuilder().taxonId(2222).scientificName("value2").build();

        SuggestDocument result =
                OrganismToSuggestDocument.getOrganismSuggestDocument(organism, "test2");
        assertNotNull(result);

        assertEquals("test2", result.dictionary);
        assertEquals("2222", result.id);
        assertEquals("value2", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testGetOrganismSuggestDocumentWithAltValues() {
        TaxonomyLineage organism =
                new TaxonomyLineageBuilder()
                        .taxonId(2222)
                        .scientificName("value2")
                        .commonName("alt1")
                        .addSynonyms("alt2")
                        .addSynonyms("alt3")
                        .build();

        SuggestDocument result =
                OrganismToSuggestDocument.getOrganismSuggestDocument(organism, "test2");
        assertNotNull(result);

        assertEquals("test2", result.dictionary);
        assertEquals("2222", result.id);
        assertEquals("value2", result.value);
        assertTrue(result.altValues.contains("alt1"));
        assertTrue(result.altValues.contains("alt2"));
        assertTrue(result.altValues.contains("alt3"));
        assertEquals("medium", result.importance);
    }
}
