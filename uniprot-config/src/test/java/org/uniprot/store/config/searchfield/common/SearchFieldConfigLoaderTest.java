package org.uniprot.store.config.searchfield.common;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.SearchFieldItem;

public class SearchFieldConfigLoaderTest {

    private static SearchFieldConfigLoader loader;

    @BeforeAll
    static void setUp() {
        loader = new SearchFieldConfigLoader();
    }

    @Test
    void testSearchFieldsConfigRead() {
        Assertions.assertNotNull(
                loader.readConfig(TestSearchFieldConfig.TEST_SEARCH_FIELDS_CONFIG));
    }

    @Test
    void testSearchFieldsConfigReadFail() {
        Assertions.assertNull(loader.readConfig("random.json"));
    }

    @Test
    void testLoadSearchFields() {
        List<SearchFieldItem> fieldItems =
                loader.loadAndGetFieldItems(TestSearchFieldConfig.TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertEquals(432, fieldItems.size());
    }

    @Test
    void testBuildIdFieldItemMap() {
        List<SearchFieldItem> fieldItems =
                loader.loadAndGetFieldItems(TestSearchFieldConfig.TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
        Map<String, SearchFieldItem> idFieldItemMap = loader.buildIdFieldItemMap(fieldItems);
        Assertions.assertNotNull(idFieldItemMap);
        Assertions.assertEquals(fieldItems.size(), idFieldItemMap.keySet().size());
    }

    @Test
    void testLoadAndGetFieldItemsFail() {
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> loader.loadAndGetFieldItems("random.json"));
    }
}
