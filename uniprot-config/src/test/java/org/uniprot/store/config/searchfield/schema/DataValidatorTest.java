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
import org.uniprot.store.config.searchfield.factory.SearchFieldConfigFactory;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;
import org.uniprot.store.config.searchfield.model.SearchFieldItemType;

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
        List<SearchFieldItem> fieldItems =
                loader.loadAndGetFieldItems(SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Map<String, SearchFieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertDoesNotThrow(
                () -> DataValidator.validateParentExists(fieldItems, idFieldsMap));
    }

    @Test
    void testFieldItemsSeqNumbers() {
        List<SearchFieldItem> fieldItems =
                loader.loadAndGetFieldItems(SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertDoesNotThrow(() -> DataValidator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsChildNumbers() {
        List<SearchFieldItem> fieldItems =
                loader.loadAndGetFieldItems(SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertDoesNotThrow(() -> DataValidator.validateChildNumbers(fieldItems));
    }

    @ParameterizedTest
    @MethodSource("provideSearchConfigFile")
    void testSortFieldIdIsValidId(String configFile) {
        List<SearchFieldItem> fieldItems = loader.loadAndGetFieldItems(configFile);
        Assertions.assertFalse(fieldItems.isEmpty());
        Map<String, SearchFieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertDoesNotThrow(
                () -> DataValidator.validateSortFieldIds(fieldItems, idFieldsMap));
    }

    @Test
    void testParentIdIsInvalidId() {
        // create few fields with non-existing parentId
        SearchFieldItem fi1 = getFieldItem("id1", "parentId");
        SearchFieldItem fi2 = getFieldItem("id2", "invalidParentId");
        SearchFieldItem fi3 = getFieldItem("id3", null);
        SearchFieldItem p1 = getFieldItem("parentId", null);
        List<SearchFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, p1);
        Map<String, SearchFieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateParentExists(fieldItems, idFieldsMap));
    }

    @Test
    void testNegativeSeqNumber() {
        SearchFieldItem fi1 = getFieldItem("id1", 0);
        SearchFieldItem fi2 = getFieldItem("id2", 1);
        SearchFieldItem fi3 = getFieldItem("id3", 2);
        SearchFieldItem fi4 = getFieldItem("id4", -3);
        List<SearchFieldItem> fieldItems = Arrays.asList(fi1, fi2, fi3, fi4);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateSeqNumbers(fieldItems));
    }

    @Test
    void testFieldItemsMissingChildNumbers() {
        SearchFieldItem p1 = getFieldItem("p1", null);
        SearchFieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        SearchFieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        SearchFieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch3.setChildNumber(1);
        List<SearchFieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateChildNumbers(fieldItems));
    }

    @Test
    void testFieldItemsInvalidChildNumbersSequence() {
        SearchFieldItem p1 = getFieldItem("p1", null);
        SearchFieldItem p1Ch1 = getFieldItem("p1ch1", "p1");
        SearchFieldItem p1Ch2 = getFieldItem("p1ch2", "p1");
        SearchFieldItem p1Ch3 = getFieldItem("p1ch3", "p1");
        p1Ch1.setChildNumber(0);
        p1Ch2.setChildNumber(3);
        p1Ch3.setChildNumber(1);
        List<SearchFieldItem> fieldItems = Arrays.asList(p1, p1Ch1, p1Ch2, p1Ch3);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateChildNumbers(fieldItems));
    }

    @Test
    void testSortFieldIdIsInvalidId() {
        SearchFieldItem f1 = getFieldItem("f1", null);
        f1.setSortFieldId("s1");
        SearchFieldItem f2 = getFieldItem("f2", null);
        f2.setSortFieldId("s2"); // invalid sort id
        SearchFieldItem f3 = getFieldItem("f3", null);
        SearchFieldItem s1 = getFieldItem("s1", null);
        List<SearchFieldItem> fieldItems = Arrays.asList(f1, f2, f3, s1);
        Map<String, SearchFieldItem> idFieldsMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertThrows(
                SearchFieldValidationException.class,
                () -> DataValidator.validateSortFieldIds(fieldItems, idFieldsMap));
    }

    private SearchFieldItem getFieldItem(String id, int seqNumber) {
        SearchFieldItemType itemType = SearchFieldItemType.group;
        String label = "Dummy Label";
        SearchFieldItem fi = getFieldItem(id, "");
        fi.setSeqNumber(seqNumber);
        fi.setItemType(itemType);
        fi.setLabel(label);
        return fi;
    }

    private SearchFieldItem getFieldItem(String id, String parentId) {
        SearchFieldItem fi = new SearchFieldItem();
        fi.setId(id);
        if (StringUtils.isNotEmpty(parentId)) {
            fi.setParentId(parentId);
        }
        return fi;
    }

    private static Stream<Arguments> provideSearchConfigFile() {
        return Stream.of(
                Arguments.of(SearchFieldConfigFactory.CROSSREF_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.DISEASE_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.GENECENTRIC_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.KEYWORD_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.LITERATURE_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.PROTEOME_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.SUBCELLLOCATION_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.SUGGEST_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.TAXONOMY_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.UNIPARC_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.UNIPROTKB_CONFIG_FILE),
                Arguments.of(SearchFieldConfigFactory.UNIREF_CONFIG_FILE));
    }
}
