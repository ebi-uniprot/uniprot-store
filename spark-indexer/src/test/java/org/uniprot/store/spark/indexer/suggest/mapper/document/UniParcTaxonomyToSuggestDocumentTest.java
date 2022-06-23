package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.Optional;
import org.junit.jupiter.api.Test;
import org.uniprot.core.taxonomy.TaxonomyLineage;
import org.uniprot.core.taxonomy.impl.TaxonomyLineageBuilder;
import org.uniprot.store.search.document.suggest.SuggestDictionary;
import org.uniprot.store.search.document.suggest.SuggestDocument;

import scala.Tuple2;

/**
 * @author sahmad
 * @since 2020-08-25
 */
class UniParcTaxonomyToSuggestDocumentTest {

    @Test
    void testTaxonomyToSuggestDocumentWithoutLineage() throws Exception {
        TaxonomyToSuggestDocument mapper =
                new TaxonomyToSuggestDocument(SuggestDictionary.UNIPARC_TAXONOMY);
        Tuple2<String, Tuple2<String, Optional<List<TaxonomyLineage>>>> tuple =
                new Tuple2<>("1111", new Tuple2<>("1111", Optional.empty()));
        Iterator<Tuple2<String, SuggestDocument>> results = mapper.call(tuple);
        assertNotNull(results);
        List<Tuple2<String, SuggestDocument>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertEquals(1, resultList.size());

        assertEquals("1111", resultList.get(0)._1);
        SuggestDocument result = resultList.get(0)._2;
        assertEquals("UNIPARC_TAXONOMY", result.dictionary);
        assertEquals("1111", result.id);
        assertNull(result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);
    }

    @Test
    void testTaxonomyToSuggestDocumentMultiplesOrganism() throws Exception {
        List<TaxonomyLineage> input = new ArrayList<>();
        TaxonomyLineage organism =
                new TaxonomyLineageBuilder().taxonId(1111).scientificName("value").build();
        input.add(organism);

        TaxonomyLineage organism2 =
                new TaxonomyLineageBuilder()
                        .taxonId(2222)
                        .scientificName("value2")
                        .commonName("altValue2")
                        .build();
        input.add(organism2);

        TaxonomyToSuggestDocument mapper =
                new TaxonomyToSuggestDocument(SuggestDictionary.UNIPARC_TAXONOMY);
        Tuple2<String, Tuple2<String, Optional<List<TaxonomyLineage>>>> tuple =
                new Tuple2<>("1111", new Tuple2<>("1111", Optional.of(input)));
        Iterator<Tuple2<String, SuggestDocument>> results = mapper.call(tuple);
        assertNotNull(results);
        List<Tuple2<String, SuggestDocument>> resultList = new ArrayList<>();
        results.forEachRemaining(resultList::add);
        assertEquals(2, resultList.size());

        assertEquals("1111", resultList.get(0)._1);
        SuggestDocument result = resultList.get(0)._2;
        assertEquals("UNIPARC_TAXONOMY", result.dictionary);
        assertEquals("1111", result.id);
        assertEquals("value", result.value);
        assertTrue(result.altValues.isEmpty());
        assertEquals("medium", result.importance);

        assertEquals("2222", resultList.get(1)._1);
        result = resultList.get(1)._2;
        assertEquals("UNIPARC_TAXONOMY", result.dictionary);
        assertEquals("2222", result.id);
        assertEquals("value2", result.value);
        assertTrue(result.altValues.contains("altValue2"));
        assertEquals("medium", result.importance);
    }
}
