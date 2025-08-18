package org.uniprot.store.spark.indexer.suggest.mapper.document;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.uniprot.core.cv.disease.DiseaseEntry;
import org.uniprot.core.cv.disease.impl.DiseaseEntryBuilder;
import org.uniprot.store.search.document.suggest.SuggestDocument;

class DiseaseToSuggestDocumentTest {

    @Test
    void testDiseaseToSuggestDocumentWithoutAltNamesAndAcronym() throws Exception {
        DiseaseEntry diseaseEntry =
                new DiseaseEntryBuilder()
                        .id("DI-12345")
                        .name("Disease Alpha")
                        .alternativeNamesSet(Collections.emptyList())
                        .acronym(null)
                        .build();

        DiseaseToSuggestDocument mapper = new DiseaseToSuggestDocument();
        SuggestDocument result = mapper.call(diseaseEntry);

        assertEquals("DISEASE", result.dictionary);
        assertEquals("DI-12345", result.id);
        assertEquals("Disease Alpha", result.value);
        assertTrue(result.altValues.isEmpty());
    }

    @Test
    void testDiseaseToSuggestDocumentWithAltNamesAndAcronym() throws Exception {
        List<String> altNames = List.of("Alt Disease Name 1", "Alt Disease Name 2");
        DiseaseEntry diseaseEntry =
                new DiseaseEntryBuilder()
                        .id("DI-67890")
                        .name("Disease Beta")
                        .alternativeNamesSet(altNames)
                        .acronym("DBA")
                        .build();

        DiseaseToSuggestDocument mapper = new DiseaseToSuggestDocument();
        SuggestDocument result = mapper.call(diseaseEntry);

        assertEquals("DISEASE", result.dictionary);
        assertEquals("DI-67890", result.id);
        assertEquals("Disease Beta", result.value);
        assertEquals(3, result.altValues.size());
        assertTrue(result.altValues.containsAll(altNames));
        assertTrue(result.altValues.contains("DBA"));
    }

    @Test
    void testDiseaseToSuggestDocumentWithoutAltNames() throws Exception {
        DiseaseEntry diseaseEntry =
                new DiseaseEntryBuilder()
                        .id("DI-67890")
                        .name("Disease Beta")
                        .acronym("DBA")
                        .build();

        DiseaseToSuggestDocument mapper = new DiseaseToSuggestDocument();
        SuggestDocument result = mapper.call(diseaseEntry);

        assertEquals("DISEASE", result.dictionary);
        assertEquals("DI-67890", result.id);
        assertEquals("Disease Beta", result.value);
        assertEquals(1, result.altValues.size());
        assertTrue(result.altValues.contains("DBA"));
    }
}
