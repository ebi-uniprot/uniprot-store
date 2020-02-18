//package org.uniprot.store.config.uniprotkb;
//
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.DisplayName;
//import org.junit.jupiter.api.Test;
//import org.uniprot.store.config.common.FieldConfiguration;
//import org.uniprot.store.config.model.FieldItem;
//
//import java.util.List;
//import java.util.Map;
//
//public class UniProtSearchFieldConfigurationTest {
//
//    private static final String TEST_SEARCH_FIELDS_CONFIG =
//            "src/test/resources/test-uniprot-fields.json";
//
//    @Test
//    void testUniProtSearchFieldConfigurationLoad(){
//        FieldConfiguration config = UniProtFieldConfiguration.getInstance();
//        Assertions.assertNotNull(config);
//    }
//    @DisplayName("Test loading non-existent config file")
//    @Test
//    void testSearchFieldsConfigReadFail() {
//        FieldConfiguration config = TestFieldConfiguration.getInstance();
//        Assertions.assertThrows(
//                IllegalArgumentException.class, () -> config.loadAndGetFieldItems("random.json"));
//    }
//
//    @DisplayName("Test read search fields from config file")
//    @Test
//    void testLoadSearchFields() {
//        FieldConfiguration config = TestFieldConfiguration.getInstance();
//        List<FieldItem> fieldItems = config.getAllFieldItems();
//        Assertions.assertNotNull(fieldItems);
//    }
//
//    @DisplayName("Test build id to fieldItem Map from the list of fieldItems")
//    @Test
//    void testBuildIdFieldItemMap() {
//        FieldConfiguration config = UniProtFieldConfiguration.getInstance();
//        List<FieldItem> fieldItems = config.loadAndGetFieldItems(TEST_SEARCH_FIELDS_CONFIG);
//        Assertions.assertNotNull(fieldItems);
//        Map<String, FieldItem> idFieldItemMap = config.buildIdFieldItemMap(fieldItems);
//        Assertions.assertNotNull(idFieldItemMap);
//    }
//
//    @DisplayName("Test get field by id")
//    @Test
//    void testGetFieldItemById() {
//        FieldConfiguration config = TestFieldConfiguration.getInstance();
//        String id = "ptmproc";
//        FieldItem fieldItem = config.getFieldItemById(id);
//        Assertions.assertNotNull(fieldItem);
//        Assertions.assertEquals(id, fieldItem.getId());
//    }
//
//    @DisplayName("Test get non-existent field by id")
//    @Test
//    void testGetFieldItemByNonExistingId() {
//        FieldConfiguration config = TestFieldConfiguration.getInstance();
//        String id = "random";
//        FieldItem fieldItem = config.getFieldItemById(id);
//        Assertions.assertNull(fieldItem);
//    }
//
//    @DisplayName("Test get all field items")
//    @Test
//    void testGetAllFieldItems() {
//        FieldConfiguration config = TestFieldConfiguration.getInstance();
//        List<FieldItem> fields = config.getAllFieldItems();
//        Assertions.assertNotNull(fields);
//        Assertions.assertFalse(fields.isEmpty());
//    }
//}
