// package org.uniprot.store.config.searchfield.impl;
//
// import java.util.List;
//
// import org.junit.jupiter.api.Assertions;
// import org.junit.jupiter.api.BeforeAll;
// import org.junit.jupiter.api.DisplayName;
// import org.junit.jupiter.api.Test;
// import org.uniprot.store.config.searchfield.common.SearchFieldConfig;
// import org.uniprot.store.config.searchfield.model.FieldItem;
//
// public class UniProtKBSearchFieldConfigurationTest {
//
//    private static SearchFieldConfig fieldConfig;
//
//    @BeforeAll
//    static void setUp() {
//        fieldConfig = UniProtKBSearchFieldConfiguration.getInstance();
//    }
//
//    @DisplayName("Test get top level fields")
//    @Test
//    void testGetTopLevelFields() {
//        List<FieldItem> roots = fieldConfig.getTopLevelFieldItems();
//        Assertions.assertNotNull(roots);
//        Assertions.assertFalse(roots.isEmpty());
//    }
//
//    @DisplayName("Test get children by parentId")
//    @Test
//    void testGetChildFields() {
//        String id = "structure";
//        List<FieldItem> fields = fieldConfig.getChildFieldItems(id);
//        Assertions.assertNotNull(fields);
//        Assertions.assertFalse(fields.isEmpty());
//    }
//
//    @DisplayName("Test try to get child of field with no child")
//    @Test
//    void testGetNoChildFields() {
//        String id = "length_sort";
//        List<FieldItem> fields = fieldConfig.getChildFieldItems(id);
//        Assertions.assertNotNull(fields);
//        Assertions.assertTrue(fields.isEmpty());
//    }
//
//    @DisplayName("Test try to get child of non-existent parent field")
//    @Test
//    void testGetChildFieldsWithWrongParent() {
//        String id = "random";
//        List<FieldItem> fields = fieldConfig.getChildFieldItems(id);
//        Assertions.assertNotNull(fields);
//        Assertions.assertTrue(fields.isEmpty());
//    }
// }
