package org.uniprot.store.search.domain2;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.search.domain2.impl.SearchFieldImpl;
import org.uniprot.store.search.field.SearchFields;

public class UniProtKBSearchFieldsTest {

    private static SearchFields searchFields;

    @BeforeAll
    static void setUp() {
        searchFields = new UniProtKBSearchFields();
    }

    @Test
    void testGetSearchFields() {
        Set<SearchField> allSearchFields = searchFields.getSearchFields();
        Assertions.assertNotNull(allSearchFields);
        Assertions.assertFalse(allSearchFields.isEmpty());
        List<SearchField> dbXrefCountFields =
                allSearchFields.stream()
                        .filter(f -> f.getName().startsWith(SearchFieldImpl.XREF_COUNT_PREFIX))
                        .collect(Collectors.toList());
        Assertions.assertTrue(dbXrefCountFields.size() > 0);
        Assertions.assertTrue(allSearchFields.size() > dbXrefCountFields.size());
    }

    @ParameterizedTest
    @MethodSource("provideFieldForHasSortField")
    void testGetHasSortField(String field, boolean expected, String errorMessage) {
        Assertions.assertEquals(
                expected, searchFields.hasSortField(field), errorMessage + "'" + field + "'");
    }

    @ParameterizedTest
    @MethodSource("provideFieldForGetSortFieldsFor")
    void testGetSortFieldFor(String field, String sortFieldName) {
        SearchField sortField = searchFields.getSortFieldFor(field);
        Assertions.assertNotNull(sortField, "Sort field not found for field '" + field + "'");
        Assertions.assertEquals(
                sortFieldName,
                sortField.getName(),
                "Expected sortField name '" + sortFieldName + "'");
    }

    @Test
    void testGetSortFieldForFieldWithoutSortField() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> searchFields.getSortFieldFor("taxonomy_id"));
    }

    @Test
    void testGetSortFields() {
        Set<SearchField> allSortFields = searchFields.getSortFields();
        Assertions.assertNotNull(allSortFields);
        Assertions.assertEquals(8, allSortFields.size());
    }

    private static Stream<Arguments> provideFieldForGetSortFieldsFor() {
        return Stream.of(
                Arguments.of("organism_name", "organism_sort"),
                Arguments.of("accession", "accession_id"),
                Arguments.of("mnemonic", "mnemonic_sort"),
                Arguments.of("name", "name_sort"),
                Arguments.of("gene", "gene_sort"),
                Arguments.of("annotation_score", "annotation_score"),
                Arguments.of("mass", "mass"),
                Arguments.of("length", "length"));
    }

    private static Stream<Arguments> provideFieldForHasSortField() {
        String notFoundErrMessage = "Sort field not found for field ";
        String foundErrMessage = "Sort field found for field ";
        return Stream.of(
                Arguments.of("organism_name", true, notFoundErrMessage),
                Arguments.of("accession", true, notFoundErrMessage),
                Arguments.of("mnemonic", true, notFoundErrMessage),
                Arguments.of("name", true, notFoundErrMessage),
                Arguments.of("gene", true, notFoundErrMessage),
                Arguments.of("annotation_score", true, notFoundErrMessage),
                Arguments.of("mass", true, notFoundErrMessage),
                Arguments.of("length", true, notFoundErrMessage),
                Arguments.of("taxonomy_id", false, foundErrMessage));
    }
}
