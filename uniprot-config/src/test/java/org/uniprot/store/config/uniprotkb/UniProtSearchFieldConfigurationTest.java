package org.uniprot.store.config.uniprotkb;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.uniprot.store.config.model.FieldItem;

public class UniProtSearchFieldConfigurationTest {

    private static final String TEST_SEARCH_FIELDS_CONFIG =
            "src/test/resources/test-uniprot-fields.json";

    @DisplayName("Test loading non-existent config file")
    @Test
    void testSearchFieldsConfigReadFail() {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> config.loadAndGetFieldItems("random.json"));
    }

    @DisplayName("Test read search fields from config file")
    @Test
    void testLoadSearchFields() {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        List<FieldItem> fieldItems = config.loadAndGetFieldItems(TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
    }

    @DisplayName("Test build id to fieldItem Map from the list of fieldItems")
    @Test
    void testBuildIdFieldItemMap() throws Exception {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        List<FieldItem> fieldItems = config.loadAndGetFieldItems(TEST_SEARCH_FIELDS_CONFIG);
        Assertions.assertNotNull(fieldItems);
        Map<String, FieldItem> idFieldItemMap = config.buildIdFieldItemMap(fieldItems);
        Assertions.assertNotNull(idFieldItemMap);
    }

    @DisplayName("Test get field by id")
    @Test
    void testGetFieldItemById() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "ptmproc";
        FieldItem fieldItem = config.getFieldItemById(id);
        Assertions.assertNotNull(fieldItem);
        Assertions.assertEquals(id, fieldItem.getId());
    }

    @DisplayName("Test get non-existent field by id")
    @Test
    void testGetFieldItemByNonExistingId() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "random";
        FieldItem fieldItem = config.getFieldItemById(id);
        Assertions.assertNull(fieldItem);
    }

    @DisplayName("Test get top level fields")
    @Test
    void testGetTopLevelFields() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        List<FieldItem> roots = config.getTopLevelFieldItems();
        Assertions.assertNotNull(roots);
        Assertions.assertFalse(roots.isEmpty());
    }

    @DisplayName("Test get children by parentId")
    @Test
    void testGetChildFields() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "structure";
        List<FieldItem> fields = config.getChildFieldItems(id);
        Assertions.assertNotNull(fields);
        Assertions.assertFalse(fields.isEmpty());
    }

    @DisplayName("Test try to get child of field with no child")
    @Test
    void testGetNoChildFields() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "length_sort";
        List<FieldItem> fields = config.getChildFieldItems(id);
        Assertions.assertNotNull(fields);
        Assertions.assertTrue(fields.isEmpty());
    }

    @DisplayName("Test try to get child of non-existent parent field")
    @Test
    void testGetChildFieldsWithWrongParent() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "random";
        List<FieldItem> fields = config.getChildFieldItems(id);
        Assertions.assertNotNull(fields);
        Assertions.assertTrue(fields.isEmpty());
    }

    @DisplayName("Test get all field items")
    @Test
    void testGetAllFieldItems() throws IOException {
        UniProtSearchFieldConfiguration config = UniProtSearchFieldConfiguration.getInstance();
        config.initForTesting(TEST_SEARCH_FIELDS_CONFIG);
        String id = "random";
        List<FieldItem> fields = config.getAllFieldItems();
        Assertions.assertNotNull(fields);
        Assertions.assertFalse(fields.isEmpty());
    }
}
