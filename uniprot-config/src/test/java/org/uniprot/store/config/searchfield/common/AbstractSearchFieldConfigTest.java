package org.uniprot.store.config.searchfield.common;

import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.FieldType;

public class AbstractSearchFieldConfigTest {

    private static SearchFieldConfig testFieldConfig;

    @BeforeAll
    static void setUp() {
        testFieldConfig = TestSearchFieldConfig.getInstance();
    }

    @Test
    void testGetAllFieldItems() {
        List<FieldItem> fieldItems = testFieldConfig.getAllFieldItems();
        Assertions.assertNotNull(fieldItems);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertEquals(432, fieldItems.size());
    }

    @Test
    void testGetSearchFieldItemByName() {
        String fieldName = "annotation_score";
        FieldItem annotScore = testFieldConfig.getSearchFieldItemByName(fieldName);
        Assertions.assertNotNull(annotScore);
        Assertions.assertEquals(FieldType.general, annotScore.getFieldType());
        Assertions.assertEquals(fieldName, annotScore.getFieldName());
    }

    @Test
    void testGetSearchFieldItemByNameWithNonExistentField() {
        String fieldName = "some random field name";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getSearchFieldItemByName(fieldName));
    }

    @Test
    void testIsSearchFieldValueValid() {
        String fieldName = "accession_id";
        String value = "P12345";
        Assertions.assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testIsSearchFieldValueValidWithInvalidValue() {
        String fieldName = "accession_id";
        String invalidValue = "PP12345";
        Assertions.assertFalse(testFieldConfig.isSearchFieldValueValid(fieldName, invalidValue));
    }

    @Test
    void testIsSearchFieldValueValidWithInvalidField() {
        String fieldName = "some random field";
        String invalidValue = "PP12345";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.isSearchFieldValueValid(fieldName, invalidValue));
    }

    @Test
    void testIsSearchFieldValueValidWithoutRegex() {
        String fieldName = "content";
        String value = "some random value";
        Assertions.assertTrue(testFieldConfig.isSearchFieldValueValid(fieldName, value));
    }

    @Test
    void testDoesSearchFieldItemExist() {
        String fieldName = "go";
        Assertions.assertTrue(testFieldConfig.doesSearchFieldItemExist(fieldName));
    }

    @Test
    void testDoesSearchFieldItemExistWithNonExistingField() {
        String fieldName = "some random non-existing field";
        Assertions.assertFalse(testFieldConfig.doesSearchFieldItemExist(fieldName));
    }

    @Test
    void testGetCorrespondingSortField() {
        String searchFieldName = "mnemonic";
        String expectedSortFieldName = "mnemonic_sort";
        FieldItem sortField = testFieldConfig.getCorrespondingSortField(searchFieldName);
        Assertions.assertNotNull(sortField);
        Assertions.assertEquals(FieldType.sort, sortField.getFieldType());
        Assertions.assertEquals(expectedSortFieldName, sortField.getFieldName());
    }

    @Test
    void testGetCorrespondingSortFieldWithNonExistingSortField() {
        String searchFieldName = "some random search field";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getCorrespondingSortField(searchFieldName));
    }

    @Test
    void testDoesCorrespondingSortFieldExist() {
        String searchFieldName = "mnemonic";
        Assertions.assertTrue(testFieldConfig.doesCorrespondingSortFieldExist(searchFieldName));
    }

    @Test
    void testDoesCorrespondingSortFieldExistWithNonExistingSortField() {
        String searchFieldName = "some random search field";
        Assertions.assertFalse(testFieldConfig.doesCorrespondingSortFieldExist(searchFieldName));
    }

    @Test
    void testGetSortFieldItems() {
        List<FieldItem> sortFields = testFieldConfig.getSortFieldItems();
        Assertions.assertNotNull(sortFields);
        Assertions.assertFalse(sortFields.isEmpty());
        sortFields.stream()
                .forEach(fi -> Assertions.assertTrue(FieldType.sort == fi.getFieldType()));
    }

    @Test
    void testGetFieldTypeByFieldNameOfEvidence() {
        String fieldName = "ccev_webresource";
        FieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        Assertions.assertEquals(FieldType.general, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfGeneral() {
        String fieldName = "ccev_webresource";
        FieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        Assertions.assertEquals(FieldType.general, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfRange() {
        String fieldName = "lit_pubdate";
        FieldType fieldType = testFieldConfig.getFieldTypeBySearchFieldName(fieldName);
        Assertions.assertEquals(FieldType.range, fieldType);
    }

    @Test
    void testGetFieldTypeByFieldNameOfSort() {
        String fieldName = "name_sort";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getFieldTypeBySearchFieldName(fieldName));
    }

    @Test
    void testGetFieldTypeByFieldNameOfInvalidName() {
        String fieldName = "some random field name";
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> testFieldConfig.getFieldTypeBySearchFieldName(fieldName));
    }
}
