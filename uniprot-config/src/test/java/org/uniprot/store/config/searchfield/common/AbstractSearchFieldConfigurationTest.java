package org.uniprot.store.config.searchfield.common;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.searchfield.model.FieldItem;

public class AbstractSearchFieldConfigurationTest {

    private static SearchFieldConfig fieldConfig;

    @BeforeAll
    static void setUp() {
        fieldConfig = TestFieldConfiguration.getInstance();
    }

    @Test
    void testGetAllFieldItems() {
        List<FieldItem> fieldItems = fieldConfig.getAllFieldItems();
        Assertions.assertNotNull(fieldItems);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertEquals(432, fieldItems.size());
    }

    @Test
    void testLoadSearchFields() {
        List<FieldItem> fieldItems =
                fieldConfig.loadAndGetFieldItems(TestFieldConfiguration.TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
        Assertions.assertFalse(fieldItems.isEmpty());
        Assertions.assertEquals(432, fieldItems.size());
    }

    @Test
    void testBuildIdFieldItemMap() {
        List<FieldItem> fieldItems =
                fieldConfig.loadAndGetFieldItems(TestFieldConfiguration.TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
        Map<String, FieldItem> idFieldItemMap = fieldConfig.buildIdFieldItemMap(fieldItems);
        Assertions.assertNotNull(idFieldItemMap);
        Assertions.assertEquals(fieldItems.size(), idFieldItemMap.keySet().size());
    }

    @Test
    void testLoadAndGetFieldItemsFail() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> fieldConfig.loadAndGetFieldItems("random.json"));
    }

    @Test
    void testSearchFieldsConfigReadFail() {
        Assertions.assertNull(fieldConfig.readConfig("random.json"));
    }
}
