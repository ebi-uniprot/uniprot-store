package org.uniprot.store.config.searchfield.common;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldType;

class AbstractSearchFieldConfigTest {

    private static SearchFieldConfig testFieldConfig;

    @BeforeAll
    static void setUp() {
        testFieldConfig = TestSearchFieldConfig.getInstance();
    }

    @Test
    void testGetAllFieldItems() {
        List<SearchFieldItem> fieldItems = testFieldConfig.getAllFieldItems();
        assertNotNull(fieldItems);
        assertFalse(fieldItems.isEmpty());
        assertEquals(333, fieldItems.size());
    }

    @Test
    void testSearchFieldNames() {
        Set<String> fieldNames = testFieldConfig.getSearchFieldNames();
        assertNotNull(fieldNames);
        assertFalse(fieldNames.isEmpty());
        assertEquals(218, fieldNames.size());
    }

    @Test
    void testGetSearchFieldItemByName() {
        String fieldName = "annotation_score";
        SearchFieldItem annotScore = testFieldConfig.getSearchFieldItemByName(fieldName);
        assertNotNull(annotScore);
        assertEquals(SearchFieldType.GENERAL, annotScore.getFieldType());
        assertEquals(fieldName, annotScore.getFieldName());
    }

    @Test
    void testGetSearchFieldItemByNameWithNonExistentField() {
        String fieldName = "some random field name";
        assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getSearchFieldItemByName(fieldName));
    }

    @Test
    void testIsSearchFieldValueValid() {
        String fieldName = "accession_id";
        String value = "P12345";
        assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testIsSearchFieldAliasValueValid() {
        String fieldName = "accession_alias";
        String value = "P12345";
        assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testIsSearchFieldAliasValueInvalid() {
        String fieldName = "accession_alias";
        String value = "bbc";
        assertFalse(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testIsSearchFieldAliasValueValidWithoutRegex() {
        String fieldName = "name_alias";
        String value = "some random";
        assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testIsSearchFieldValueValidWithInvalidValue() {
        String fieldName = "accession_id";
        String invalidValue = "PP12345";
        assertFalse(testFieldConfig.isSearchFieldValueValid(fieldName, invalidValue));
    }

    @Test
    void testIsSearchFieldValueValidWithInvalidField() {
        String fieldName = "some random field";
        String invalidValue = "PP12345";
        assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.isSearchFieldValueValid(fieldName, invalidValue));
    }

    @Test
    void testIsSearchFieldValueValidWithoutRegex() {
        String fieldName = "content";
        String value = "some random value";
        assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testDoesSearchFieldItemExist() {
        String fieldName = "go";
        assertTrue(testFieldConfig.searchFieldItemExists(fieldName));
    }

    @Test
    void testDoesSearchFieldItemExist_givenAlias() {
        String fieldName = "go_alias";
        assertTrue(testFieldConfig.searchFieldItemExists(fieldName));
    }

    @Test
    void testDoesSearchFieldItemExistWithNonExistingField() {
        String fieldName = "some random non-existing field";
        assertFalse(testFieldConfig.searchFieldItemExists(fieldName));
    }

    @Test
    void testGetCorrespondingSortField() {
        String searchFieldName = "id";
        String expectedSortFieldName = "id_sort";
        SearchFieldItem sortField = testFieldConfig.getCorrespondingSortField(searchFieldName);
        assertNotNull(sortField);
        assertEquals(SearchFieldType.SORT, sortField.getFieldType());
        assertEquals(expectedSortFieldName, sortField.getFieldName());
    }

    @Test
    void testGetCorrespondingSortFieldWithAlias() {
        String searchFieldAlias = "name_alias";
        String expectedSortFieldName = "name_sort";
        SearchFieldItem sortField = testFieldConfig.getCorrespondingSortField(searchFieldAlias);
        assertNotNull(sortField);
        assertEquals(SearchFieldType.SORT, sortField.getFieldType());
        assertEquals(expectedSortFieldName, sortField.getFieldName());
    }

    @Test
    void testGetCorrespondingSortFieldWithNonExistingSortField() {
        String searchFieldName = "some random search field";
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> testFieldConfig.getCorrespondingSortField(searchFieldName));
        assertEquals("Unknown field name or alias: " + searchFieldName, error.getMessage());
    }

    @Test
    void testDoesCorrespondingSortFieldExist() {
        String searchFieldName = "id";
        assertTrue(testFieldConfig.correspondingSortFieldExists(searchFieldName));
    }

    @Test
    void testDoesCorrespondingSortFieldExistWithNonExistingSortField() {
        String searchFieldName = "some random search field";
        assertFalse(testFieldConfig.correspondingSortFieldExists(searchFieldName));
    }

    @Test
    void testGetSortFieldItems() {
        List<SearchFieldItem> sortFields = testFieldConfig.getSortFieldItems();
        assertNotNull(sortFields);
        assertFalse(sortFields.isEmpty());
        sortFields.forEach(fi -> assertSame(SearchFieldType.SORT, fi.getFieldType()));
    }

    @Test
    void testGetFieldTypeByFieldNameOfGeneral() {
        String fieldName = "cc_webresource";
        SearchFieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        assertEquals(SearchFieldType.GENERAL, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfExperimentalEvidence() {
        String fieldName = "cc_webresource_exp";
        SearchFieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        assertEquals(SearchFieldType.GENERAL, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfRange() {
        String fieldName = "lit_pubdate";
        SearchFieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        assertEquals(SearchFieldType.RANGE, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfSort() {
        String fieldName = "name_sort";
        assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getFieldTypeBySearchFieldName(fieldName));
    }

    @Test
    void testGetFieldTypeByFieldNameOfInvalidName() {
        String fieldName = "some random field name";
        assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getFieldTypeBySearchFieldName(fieldName));
    }

    @Test
    void findSearchFieldItemByName_whenExist() {
        String fieldName = "go";

        Optional<SearchFieldItem> searchFieldItemByName =
                testFieldConfig.findSearchFieldItemByName(fieldName);

        assertEquals(fieldName, searchFieldItemByName.get().getFieldName());
    }

    @Test
    void findSearchFieldItemByName_whenNotExist() {
        assertFalse(testFieldConfig.findSearchFieldItemByName("fieldNotExist").isPresent());
    }

    @Test
    void findSearchFieldItemByAlias_whenExist() {
        String fieldNameAlias = "go_alias";

        Optional<SearchFieldItem> searchFieldItemByAlias =
                testFieldConfig.findSearchFieldItemByAlias(fieldNameAlias);

        assertEquals("go", searchFieldItemByAlias.get().getFieldName());
    }

    @Test
    void findSearchFieldItemByAlias_whenNotExist() {
        assertFalse(testFieldConfig.findSearchFieldItemByAlias("aliasNotExist").isPresent());
    }
}
