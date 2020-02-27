package org.uniprot.store.config.searchfield.schema;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
import org.uniprot.store.config.searchfield.common.SearchFieldConfigLoader;
import org.uniprot.store.config.searchfield.common.SearchFieldValidationException;
import org.uniprot.store.config.searchfield.common.TestSearchFieldConfig;
import org.uniprot.store.config.searchfield.impl.*;
import org.uniprot.store.config.searchfield.model.FieldItem;
import org.uniprot.store.config.searchfield.model.ItemType;

import com.fasterxml.jackson.core.JsonProcessingException;

public class DataValidatorTest {

    private static SearchFieldConfig fieldConfig;
    private static SearchFieldConfigLoader loader;

    @BeforeAll
    static void globalSetUp() {
        fieldConfig = TestSearchFieldConfig.getInstance();
        loader = new SearchFieldConfigLoader();
    }

    @Test
    void testParentIdIsValidId() {
        List<FieldItem> fieldItems =
                loader.loadAndGetFieldItems(UniProtKBSearchFieldConfig.CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Map<String, FieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertDoesNotThrow(
                () -> DataValidator.validateParentExists(fieldItems, idFieldsMap));
    }

    @Test
    void testFieldItemsSeqNumbers() {
        List<FieldItem> fieldItems =
                loader.loadAndGetFieldItems(UniProtKBSearchFieldConfig.CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertDoesNotThrow(() -> DataValidator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsChildNumbers() {
        List<FieldItem> fieldItems =
                loader.loadAndGetFieldItems(UniProtKBSearchFieldConfig.CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertDoesNotThrow(() -> DataValidator.validateChildNumbers(fieldItems));
    }

    @ParameterizedTest
    @MethodSource("provideSearchConfigFile")
    void testSortFieldIdIsValidId(String configFile) {
        List<FieldItem> fieldItems = loader.loadAndGetFieldItems(configFile);
        Assertions.assertFalse(fieldItems.isEmpty());
        Map<String, FieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertDoesNotThrow(
                () -> DataValidator.validateSortFieldIds(fieldItems, idFieldsMap));
    }

    @Test
    void testParentIdIsInvalidId() throws JsonProcessingException {
        // create few fields with non-existing parentId
        FieldItem fi1 = getFieldItem("id1", "parentId");
        FieldItem fi2 = getFieldItem("id2", "invalidParentId");
        FieldItem fi3 = getFieldItem("id3", null);
        FieldItem p1 = getFieldItem("parentId", null);
        List<FieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, p1);
        Map<String, FieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateParentExists(fieldItems, idFieldsMap));
    }

    @Test
    void testNegativeSeqNumber() {
        FieldItem fi1 = getFieldItem("id1", 0);
        FieldItem fi2 = getFieldItem("id2", 1);
        FieldItem fi3 = getFieldItem("id3", 2);
        FieldItem fi4 = getFieldItem("id4", -3);
        List<FieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, fi4);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsMissingChildNumbers() {
        FieldItem p1 = getFieldItem("p1", null);
        FieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        FieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        FieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch3.setChildNumber(1);
        List<FieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateChildNumbers(fieldItems));
    }

    @Test
    void testFieldItemsInvalidChildNumbersSequence() {
        FieldItem p1 = getFieldItem("p1", null);
        FieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        FieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        FieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch2.setChildNumber(3);
        p1Ch3.setChildNumber(1);
        List<FieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateChildNumbers(fieldItems));
    }

    @Test
    void testSortFieldIdIsInvalidId() {
        FieldItem f1 = getFieldItem("f1", null);
        f1.setSortFieldId("s1");
        FieldItem f2 = getFieldItem("f2", null);
        f2.setSortFieldId("s2"); // invalid sort id
        FieldItem f3 = getFieldItem("f3", null);
        FieldItem s1 = getFieldItem("s1", null);
        List<FieldItem> fieldItems = Arrays.asList(f1, f2, f3, s1);
        Map<String, FieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateSortFieldIds(fieldItems, idFieldsMap));
    }

    private FieldItem getFieldItem(String id, int seqNumber) {
        ItemType itemType = ItemType.group;
        String label = "Dummy Label";
        FieldItem fi = getFieldItem(id, "");
        fi.setSeqNumber(seqNumber);
        fi.setItemType(itemType);
        fi.setLabel(label);
        return fi;
    }

    private FieldItem getFieldItem(String id, String parentId) {
        FieldItem fi = new FieldItem();
        fi.setId(id);
        if (StringUtils.isNotEmpty(parentId)) {
            fi.setParentId(parentId);
        }
        return fi;
    }

    private static Stream<Arguments> provideSearchConfigFile() {
        return Stream.of(
                Arguments.of(CrossRefSearchFieldConfig.CONFIG_FILE),
                Arguments.of(DiseaseSearchFieldConfig.CONFIG_FILE),
                Arguments.of(GeneCentricSearchFieldConfig.CONFIG_FILE),
                Arguments.of(KeywordSearchFieldConfig.CONFIG_FILE),
                Arguments.of(LiteratureSearchFieldConfig.CONFIG_FILE),
                Arguments.of(ProteomeSearchFieldConfig.CONFIG_FILE),
                Arguments.of(SubcellLocationSearchFieldConfig.CONFIG_FILE),
                Arguments.of(SuggestSearchFieldConfig.CONFIG_FILE),
                Arguments.of(TaxonomySearchFieldConfig.CONFIG_FILE),
                Arguments.of(UniParcSearchFieldConfig.CONFIG_FILE),
                Arguments.of(UniProtKBSearchFieldConfig.CONFIG_FILE),
                Arguments.of(UniRefSearchFieldConfig.CONFIG_FILE));
    }
}
